// OS_STATUS: public
package com.tesora.dve.sql.infoschema.engine;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.ConstantSyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.DelegatingInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SyntheticInformationSchemaColumn;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.FunCollector;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;

// responsible for flattening view queries down to logical table queries
// and logical table queries down to either hql queries or sql queries.
public class LogicalSchemaQueryEngine {

	private static final boolean emit = Boolean.getBoolean("parser.debug");
	private static final Logger logger = Logger.getLogger(LogicalSchemaQueryEngine.class);
	
	private static boolean canLog() { return emit || logger.isDebugEnabled(); }
	
	private static void log(String in) {
		if (emit)
			System.out.println(in);
		if (logger.isDebugEnabled())
			logger.debug(in);
	}
	
	// planning entry point
	@SuppressWarnings("unchecked")
	public static IntermediateResultSet execute(SchemaContext sc, SelectStatement ss) {
		ProjectionInfo pi = ss.getProjectionMetadata(sc);
		ViewQuery vq = new ViewQuery(ss, Collections.EMPTY_MAP, null);
		annotate(sc,vq,ss);
		return buildResultSet(sc, vq, pi);
	}
	
	// main entry point.  
	public static IntermediateResultSet buildResultSet(SchemaContext sc, ViewQuery vq, ProjectionInfo pi) {
		if (sc == null) return new IntermediateResultSet();
		LogicalQuery lq = convertDown(sc, vq);
		QueryExecutionKind qek = determineKind(lq);
		if (qek != QueryExecutionKind.RAW)
			return buildCatalogEntities(sc, lq, qek).getResultSet(sc);
		return buildRawResultSet(sc, lq, pi);
	}

	public static EntityResults buildCatalogEntities(SchemaContext sc, ViewQuery vq) {
		LogicalQuery lq = convertDown(sc, vq);
		QueryExecutionKind qek = determineKind(lq);
		return buildCatalogEntities(sc, lq, qek);
	}

	
	private static EntityResults buildCatalogEntities(SchemaContext sc, LogicalQuery lq, QueryExecutionKind qek) {
		if (qek == QueryExecutionKind.RAW)
			throw new InformationSchemaException("Unable to execute info schema query as entity query");
		List<ExpressionNode> origProjection = lq.getQuery().getProjection();
		List<CatalogEntity> ents = null;
		// clear the projection - needed for execution
		lq.getQuery().getProjectionEdge().clear();
		if (qek == QueryExecutionKind.ENTITY)
			ents = executeEntityQuery(sc, lq);
		else if (qek == QueryExecutionKind.RAW_ENTITY)
			ents = executeRawEntityQuery(sc, lq);
		lq.getQuery().getProjectionEdge().set(origProjection);
		return new EntityResults(lq, ents);		
	}
	
	
	private static List<CatalogEntity> executeEntityQuery(SchemaContext sc, LogicalQuery lq) {
		SelectStatement ss = lq.getQuery();
		// we used to build FROM .... but we're switching to SELECT ... FROM ... so that we can have
		// better control over the return result (mainly for mt)
		// recall that the projection was already cleared, so just add the table instance(s)
		ArrayList<ExpressionNode> proj = new ArrayList<ExpressionNode>(lq.getForwarding().values());
		ss.getProjectionEdge().set(proj);
        Emitter em = Singletons.require(HostService.class).getDBNative().getEmitter();
		String sql = ss.getSQL(sc,em, EmitOptions.INFOSCHEMA_ENTITY, false);
		if (canLog()) {
			log("entity info schema query: '" + sql + "'");
			StringBuffer params = new StringBuffer();
			params.append("params {");
			for(Map.Entry<String, Object> me : lq.getParams().entrySet()) {
				params.append(me.getKey()).append("=>'").append(me.getValue()).append("' ");
			}
			params.append("}");
			log(params.toString());
		}
		return sc.getCatalog().query(sql, lq.getParams());
	}

	private static List<CatalogEntity> executeRawEntityQuery(SchemaContext sc, LogicalQuery lq) {
		if (lq.getForwarding().size() != 1)
			throw new InformationSchemaException("Raw entity query with more than one entity return type");
		SelectStatement ss = lq.getQuery();
		ss.getProjectionEdge().add(new Wildcard(null));
		ExpressionNode only = lq.getForwarding().values().iterator().next();
		LogicalInformationSchemaTable list = null;
		if (only instanceof TableInstance)
			list = (LogicalInformationSchemaTable) ((TableInstance)only).getTable();
		else
			throw new InformationSchemaException("hmmm");
		Class<?> entClass = list.getEntityClass();
        Emitter em = Singletons.require(HostService.class).getDBNative().getEmitter();
		String sql = ss.getSQL(sc,em, EmitOptions.INFOSCHEMA_ENTITY, false);
		if (canLog())
			log("raw entity info schema query: '" + sql + "'");
		return sc.getCatalog().nativeQuery(sql, lq.getParams(), entClass);
	}
	
	private static QueryExecutionKind determineKind(LogicalQuery lq) {
		// if any of the tables is raw this must be a raw query
		for(ExpressionNode en : lq.getForwarding().values()) {
			if (en instanceof TableInstance) {
				TableInstance ti = (TableInstance) en;
				LogicalInformationSchemaTable list = (LogicalInformationSchemaTable) ti.getTable();
				if (list.requiresRawExecution())
					return QueryExecutionKind.RAW;
			} else if (en instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) en;
				LogicalInformationSchemaTable list = (LogicalInformationSchemaTable) ci.getTableInstance().getTable();
				if (list.requiresRawExecution())
					return QueryExecutionKind.RAW;
			} else { 
				throw new InformationSchemaException("hmmm");
			}
		}
		// next check for:
		// [1] no group by, no limit, no having, no funs in projection, no unknown funs => entity query
		// [2] no funs in projection, unknown funs => raw entity query
		// [3] funs in projection => raw query
		
		ListSet<FunctionCall> funcs = FunCollector.collectFuns(lq.getQuery().getProjectionEdge());
		if (!funcs.isEmpty())
			return QueryExecutionKind.RAW;
		funcs = FunCollector.collectFuns(lq.getQuery().getEdges());
		for(FunctionCall fc : funcs) {
			if (fc.getFunctionName().isUnknown())
				return QueryExecutionKind.RAW_ENTITY;
			else if (fc.getFunctionName().isAggregate())
				return QueryExecutionKind.RAW;
		}
		
		if (lq.getQuery().getLimitEdge().has())
			return QueryExecutionKind.RAW;

		if (lq.getQuery().getHavingEdge().has() || lq.getQuery().getGroupBysEdge().has())
			return QueryExecutionKind.RAW_ENTITY;
		
		return QueryExecutionKind.ENTITY;
	}

	private static void accumulateSelectorPath(List<AbstractInformationSchemaColumnView> acc, ColumnInstance ci) {
		if (ci instanceof ScopedColumnInstance) {
			ScopedColumnInstance sci = (ScopedColumnInstance) ci;
			accumulateSelectorPath(acc,sci.getRelativeTo());
		}
		acc.add((AbstractInformationSchemaColumnView) ci.getColumn());
	}
	
	public static LogicalQuery convertDown(SchemaContext sc, ViewQuery vq) {
		SelectStatement in = vq.getQuery();
		if (canLog()) {
			String sql = in.getSQL(sc);
			log("info schema query before logical conversion: '" + sql + "'");						
		}
		if (sc != null && !sc.getPolicyContext().isRoot()) {
			ListSet<TableKey> tabs = EngineConstant.TABLES_INC_NESTED.getValue(in,sc);
			for(TableKey tk : tabs) {
				InformationSchemaTableView istv = (InformationSchemaTableView) tk.getTable();
				if (vq.getoverrideRequiresPrivilegeValue() != null) {
					if (vq.getoverrideRequiresPrivilegeValue()) {
						throw new InformationSchemaException("You do not have permissions to query " + istv.getName().get());
					}
				} else {
					istv.assertPermissions(sc);
				}
			}
		}
		InformationSchemaTableView.derefEntities(in);
		List<ExpressionNode> origProjection = vq.getQuery().getProjection();
		ArrayList<List<AbstractInformationSchemaColumnView>> columns = new ArrayList<List<AbstractInformationSchemaColumnView>>();
		for(int i = 0; i < origProjection.size(); i++) {
			ExpressionNode en = origProjection.get(i);
			ExpressionNode targ = ExpressionUtils.getTarget(en);
			ArrayList<AbstractInformationSchemaColumnView> pathTo = new ArrayList<AbstractInformationSchemaColumnView>();
			if (targ instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) targ;
				accumulateSelectorPath(pathTo,ci);
			} else if (targ instanceof FunctionCall) {
				pathTo.add(new SyntheticInformationSchemaColumn(new UnqualifiedName("unknown")));
			} else if (targ instanceof LiteralExpression) {
				LiteralExpression le = (LiteralExpression)targ;
				UnqualifiedName columnName = new UnqualifiedName(PEStringUtils.dequote(vq.getQuery().getProjectionMetadata(sc).getColumnInfo(i+1).getAlias()));
				Type type = null;
				try {
					type = BasicType.buildFromLiteralExpression(sc, le);
				} catch (PEException e) {
					throw new InformationSchemaException("Cannot create a proper type for literal value '" + le.getValue(sc) + "'");
				}
				pathTo.add(new ConstantSyntheticInformationSchemaColumn(null, columnName, type, le.getValue(sc)));
			}
			columns.add(pathTo);
		}
		LogicalConverterTraversal lca = new LogicalConverterTraversal(in.getAliases());
		SelectStatement out = (SelectStatement) lca.traverse(in);
		out.getDerivedInfo().getLocalTableKeys().clear();
		for(ExpressionNode en : lca.getForwarding().values()) {
			TableInstance ti = (TableInstance) en;
			out.getDerivedInfo().addLocalTable(ti.getTableKey());
		}

		LogicalQuery lq = new LogicalQuery(vq, out, vq.getParams(), lca.getForwarding(), columns);
		
		// if any off the logical tables is layered, then we need to further rewrite		
		ListSet<TableKey> tabs = EngineConstant.TABLES.getValue(out,sc);
		ListSet<LogicalInformationSchemaTable> layered = new ListSet<LogicalInformationSchemaTable>();
		for(TableKey tk : tabs) {
			LogicalInformationSchemaTable list = (LogicalInformationSchemaTable) tk.getTable();
			if (list.isLayered())
				layered.add(list);
		}
		if (canLog() && !layered.isEmpty()) {
            Emitter em = Singletons.require(HostService.class).getDBNative().getEmitter();
			String sql = out.getSQL(sc,em, EmitOptions.INFOSCHEMA_ENTITY, false);
			log("info schema query before layered rewrites: '" + sql + "'");			
		}
		// let the layered tables do their rewrite magic
		LogicalQuery working = lq;
		for(LogicalInformationSchemaTable list : layered)
			working = list.explode(sc,working);
		
		return working;
	}
	
	private static class LogicalConverterTraversal extends Traversal {

		private Map<TableKey, ExpressionNode> forwarding;
		private AliasInformation aliases;
		
		public LogicalConverterTraversal(AliasInformation ai) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			forwarding = new LinkedHashMap<TableKey, ExpressionNode>();
			aliases = ai;
		}
		
		public Map<TableKey, ExpressionNode> getForwarding() {
			return forwarding;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof TableInstance) {
				return map((TableInstance)in);
			} else if (in instanceof ScopedColumnInstance) {
				ScopedColumnInstance sci = (ScopedColumnInstance) in;
				AbstractInformationSchemaColumnView icol = (AbstractInformationSchemaColumnView) sci.getColumn();
				ExpressionNode mapped = map(sci.getRelativeTo());
				if (mapped instanceof ColumnInstance) {
					return new ScopedColumnInstance(icol.isSynthetic() ? null : icol.getLogicalColumn(),(ColumnInstance)mapped);
				}
				// this ought to be an error - we had a synthetic column that was deref'd....
				throw new InformationSchemaException("Internal error: during logical conversion encountered a synthetic column in a scoped path");
			} else if (in instanceof ColumnInstance) {
				return map((ColumnInstance)in);
			}
			return in;		
		}
		
		private TableInstance map(TableInstance in) {
			TableInstance ti = in;
			if (ti.getTable() instanceof LogicalInformationSchemaTable)
				return ti;
			if (ti.getAlias() == null)
				ti.setAlias(aliases.buildNewAlias(new UnqualifiedName("isq")));
			TableKey itk = ti.getTableKey();
			TableInstance otk = (TableInstance) forwarding.get(itk);
			if (otk == null) {
				InformationSchemaTableView tab = (InformationSchemaTableView) ti.getTable();
				LogicalInformationSchemaTable btab = tab.getLogicalTable();
				otk = new TableInstance(btab,ti.getSpecifiedAs(null),ti.getAlias(),ti.getNode(),false);
				forwarding.put(itk,otk);
			}
			return (TableInstance) otk.copy(null);
		}
		
		private ExpressionNode map(ColumnInstance in) {
			ColumnInstance ici = in;
			if (in.getColumn() instanceof LogicalInformationSchemaColumn)
				return in;
			TableInstance oti = map(ici.getTableInstance());
			AbstractInformationSchemaColumnView icol = (AbstractInformationSchemaColumnView) ici.getColumn();
			if (icol.isBacked()) {
				LogicalInformationSchemaColumn ocol = icol.getLogicalColumn();
				if (ocol instanceof DelegatingInformationSchemaColumn) {
					DelegatingInformationSchemaColumn disc = (DelegatingInformationSchemaColumn) ocol;
					if (disc.getActualTable().equals(oti.getTable())) {
						ColumnInstance current = new ColumnInstance(disc.getPath().get(0),oti);
						for(int i = 1; i < disc.getPath().size(); i++) {
							current = new ScopedColumnInstance(disc.getPath().get(i), current);
						}
						return current;
					}
					return new ColumnInstance(ocol,oti);
				}
				return new ColumnInstance(ocol,oti);
			} else if (icol.isSynthetic()) {
				SyntheticInformationSchemaColumn synth = (SyntheticInformationSchemaColumn) icol;
				return synth.buildReplacement(in);
			} else {
				throw new InformationSchemaException("Invalid info schema view column type: " + icol);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private static IntermediateResultSet buildRawResultSet(SchemaContext sc, LogicalQuery lq, ProjectionInfo pi) {
		// raw conversion is harder than view to logical conversion.  if the logical table is itself a delegating table, we
		// need to first rewrite the query down to all logical tables without delegates, then convert to sql
		// now with the fully exploded query, we can look at ScopedColumnInstance, etc.
		SelectStatement working = lq.getQuery();
		Map<String,Object> params = lq.getParams();
		ListSet<ScopedColumnInstance> scopedColumns =
				ScopedColumnCollector.collect(working);
		if (!scopedColumns.isEmpty()) {
			JoinExploder.explodeJoins(sc, working, scopedColumns);
		}

		if (sc == null)
			return new IntermediateResultSet(new ColumnSet(),Collections.EMPTY_LIST);
		
		// it's very important to normalize here - otherwise we may end up with multiple columns with the same name
		// and hibernate helpfully flattens those down to a single value
		// i.e. a.name, b.name, c.name - the row will have the values a.name, a.name, a.name
		working.normalize(sc);
        Emitter em = Singletons.require(HostService.class).getDBNative().getEmitter();
		String sql = working.getSQL(sc,em, EmitOptions.INFOSCHEMA_RAW, false);
		if (canLog())
			log("raw info schema query: '" + sql + "'");

		List<?> out = sc.getCatalog().nativeQuery(sql, params);
		TreeMap<Integer, Object> exampleData = new TreeMap<Integer, Object>();
		ArrayList<ResultRow> rows = new ArrayList<ResultRow>();
		for(Object r : out) {
			ResultRow outrow = new ResultRow();
			if (r == null) {
				outrow.addResultColumn(null);
			} else if (r.getClass().isArray()) {
				Object[] ir = (Object[])r;
				for(int i = 0; i < ir.length; i++) { 
					outrow.addResultColumn(ir[i]);
					if (exampleData.get(i) == null && ir[i] != null)
						exampleData.put(i,ir[i]);
				}
			} else {
				if (exampleData.get(0) == null)
					exampleData.put(0, r);
				outrow.addResultColumn(r);
			}
			rows.add(outrow);
		}
		ColumnSet cs = buildProjectionMetadata(lq.getProjectionColumns(),pi,Functional.toList(exampleData.values()));
		return new IntermediateResultSet(cs,rows);
	}

	private static class ScopedColumnCollector extends GeneralCollectingTraversal {

		
		public ScopedColumnCollector() {
			super(Order.POSTORDER,ExecStyle.ONCE);
		}

		@Override
		public boolean is(LanguageNode ln) {
			return ln instanceof ScopedColumnInstance;
		}
		
		public static ListSet<ScopedColumnInstance> collect(LanguageNode ln) {
			return GeneralCollectingTraversal.collect(ln, new ScopedColumnCollector());
		}
	}
	
	public static ColumnSet buildProjectionMetadata(List<List<AbstractInformationSchemaColumnView>> projColumns) {
		return buildProjectionMetadata(projColumns,null,null);
	}
	
	public static ColumnSet buildProjectionMetadata(List<List<AbstractInformationSchemaColumnView>> projColumns,ProjectionInfo pi,List<Object> examples) {
		ColumnSet cs = new ColumnSet();
		try {
			for(int i = 0; i < projColumns.size(); i++) {
				List<AbstractInformationSchemaColumnView> p = projColumns.get(i);
				AbstractInformationSchemaColumnView typeColumn = p.get(p.size() - 1);
				AbstractInformationSchemaColumnView nameColumn = p.get(0);
				Type type = typeColumn.getType();
				if (type == null) {
					Object help = null;
					if (i < examples.size())
						help = examples.get(i);
					if (help == null)
						help = "help";
					ColumnInfo ci = pi.getColumnInfo(i+1);
					buildNativeType(cs,ci.getName(),ci.getAlias(),help);					
				} else {
                    NativeType nt = Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(type.getDataType(), true);
					cs.addColumn(nameColumn.getName().getSQL(), type.getSize(), nt.getTypeName(), type.getDataType());
				}
			}
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to build metadata for catalog result set",pe);
		}
		if (emit) {
			StringBuffer buf = new StringBuffer();
			for(int i = 1; i <= cs.size(); i++) {
				if (i > 1)
					buf.append(", ");
				buf.append(cs.getColumn(i).getName());
			}
			System.out.println("column set: " + buf.toString());
		}
		return cs;
	}

	/**
	 * @param cs
	 * @param colName
	 * @param colAlias
	 * @param in
	 * @throws PEException
	 */
	private static void buildNativeType(ColumnSet cs, String colName, String colAlias, Object in) throws PEException {
		if (in instanceof String) {
            NativeType nt = Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(java.sql.Types.VARCHAR, true);
			cs.addColumn(colAlias, 255, nt.getTypeName(), java.sql.Types.VARCHAR);
		} else if (in instanceof BigInteger) {
            NativeType nt = Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(java.sql.Types.BIGINT, true);
			cs.addColumn(colAlias, 32, nt.getTypeName(), java.sql.Types.BIGINT);
		} else {
			throw new PEException("Fill me in: type guess for result column type: " + in);
		}
	}
	
	private static void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in) {
		for(TableKey tk : in.getDerivedInfo().getLocalTableKeys()) {
			InformationSchemaTableView istv = (InformationSchemaTableView) tk.getTable();
			istv.annotate(sc, vq, in, tk);
		}
		for(ProjectingStatement ss : in.getDerivedInfo().getAllNestedQueries()) {
			if (ss instanceof UnionStatement) continue;
			annotate(sc, vq, (SelectStatement)ss);
		}
	}
	
}
