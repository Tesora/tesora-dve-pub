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
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.DelegatingInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.InformationSchemaTable;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaTable;
import com.tesora.dve.sql.infoschema.computed.ConstantComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.SyntheticComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.direct.DirectSchemaQueryEngine;
import com.tesora.dve.sql.infoschema.logical.VariablesLogicalInformationSchemaTable;
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
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.FunCollector;
import com.tesora.dve.sql.transform.execution.DDLQueryExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.NonDMLFeatureStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;

// responsible for flattening view queries down to logical table queries
// and logical table queries down to either hql queries or sql queries.
public class LogicalSchemaQueryEngine {

	static final boolean emit = Boolean.getBoolean("parser.debug");
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
	public static FeatureStep execute(SchemaContext sc, SelectStatement ss, FeaturePlanner planner) {
		ProjectionInfo pi = ss.getProjectionMetadata(sc);
		ViewQuery vq = new ViewQuery(ss, Collections.EMPTY_MAP, null);
		annotate(sc,vq,ss);
		boolean haveVariable = false;
		boolean haveOthers = false;
		for(TableKey tk : ss.getAllTableKeys()) {
			InformationSchemaTable istv = (InformationSchemaTable) tk.getTable();
			if (istv.isVariablesTable())
				haveVariable = true;
			else
				haveOthers = true;
		}
		if (!haveVariable) {
		
			InfoPlanningResults planResults = buildResults(sc, vq, pi, planner);
			if (planResults.getResults() != null) {
				final IntermediateResultSet irs = planResults.getResults();
				return new NonDMLFeatureStep(planner, null) {

					@Override
					public void scheduleSelf(PlannerContext sc, ExecutionSequence es)
							throws PEException {
						es.append(new DDLQueryExecutionStep("select",irs));					
					}

				}.withCachingFlag(false);
			} else {
				return planResults.getStep().withCachingFlag(false);
			}
		} else if (haveVariable && haveOthers) {
			throw new InformationSchemaException("Illegal information schema query: across variables table and others");
		} else {
			// all variables
			VariablesLogicalInformationSchemaTable varTab = 
					(VariablesLogicalInformationSchemaTable) Singletons.require(HostService.class).getInformationSchema().getLogical().lookup(VariablesLogicalInformationSchemaTable.TABLE_NAME);
			final Statement planned = varTab.execute(sc, vq,pi);
			return new NonDMLFeatureStep(planner, null) {

				@Override
				public void scheduleSelf(PlannerContext sc, ExecutionSequence es)
						throws PEException {
					planned.plan(sc.getContext(), es, sc.getBehaviorConfiguration());
				}
			}.withCachingFlag(false);
		}
	}
	
	
	// main entry point.  
	public static InfoPlanningResults buildResults(SchemaContext sc, ViewQuery vq, ProjectionInfo pi, FeaturePlanner planner) {
		if (sc == null) return new InfoPlanningResults(new IntermediateResultSet());
		LogicalQuery lq = convertDown(sc,vq);
		if (lq.isDirect()) {
			if (planner == null)
				throw new InformationSchemaException("Missing feature planner");
			return new InfoPlanningResults(DirectSchemaQueryEngine.buildStep(sc, lq, planner, pi));
		} else {
			LogicalCatalogQuery lcq = (LogicalCatalogQuery) lq;
			QueryExecutionKind qek = determineKind(lcq);
			if (qek != QueryExecutionKind.RAW)
				return new InfoPlanningResults(buildCatalogEntities(sc, lcq, qek).getResultSet(sc,pi));
			return new InfoPlanningResults(buildRawResultSet(sc, lcq, pi));
		}
	}
	
	// convenience entry point, transitional
	public static IntermediateResultSet buildResultSet(SchemaContext sc, ViewQuery vq, ProjectionInfo pi) {
		InfoPlanningResults ipr = buildResults(sc,vq,pi,null);
		if (ipr.getResults() != null)
			return ipr.getResults();
		throw new InformationSchemaException("No results available (executed via direct)");
	}
	
	public static EntityResults buildCatalogEntities(SchemaContext sc, ViewQuery vq) {
		LogicalQuery lq = convertDown(sc,vq);
		if (lq.isDirect())
			throw new InformationSchemaException("Unable to build catalog entities from direct info schema query");
		LogicalCatalogQuery lcq = (LogicalCatalogQuery) lq;
		QueryExecutionKind qek = determineKind(lcq);
		return buildCatalogEntities(sc, lcq, qek);
	}

	
	private static EntityResults buildCatalogEntities(SchemaContext sc, LogicalCatalogQuery lq, QueryExecutionKind qek) {
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
	
	
	private static List<CatalogEntity> executeEntityQuery(SchemaContext sc, LogicalCatalogQuery lq) {
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

	private static List<CatalogEntity> executeRawEntityQuery(SchemaContext sc, LogicalCatalogQuery lq) {
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
	
	private static QueryExecutionKind determineKind(LogicalCatalogQuery lq) {
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

	private static void accumulateSelectorPath(List<InformationSchemaColumn> acc, ColumnInstance ci) {
		if (ci instanceof ScopedColumnInstance) {
			ScopedColumnInstance sci = (ScopedColumnInstance) ci;
			accumulateSelectorPath(acc,sci.getRelativeTo());
		} 
		acc.add((InformationSchemaColumn) ci.getColumn());
	}
	
	public static LogicalQuery convertDown(SchemaContext sc, ViewQuery vq) {
		SelectStatement in = vq.getQuery();
		if (canLog()) {
			String sql = in.getSQL(sc);
			log("info schema query before logical conversion: '" + sql + "'");						
		}
		boolean haveViews = false;
		boolean haveNonViews = false;
		if (sc != null ) {
			ListSet<TableKey> tabs = EngineConstant.TABLES_INC_NESTED.getValue(in,sc);
			for(TableKey tk : tabs) {
				InformationSchemaTable istv = (InformationSchemaTable) tk.getTable();
				if (!sc.getPolicyContext().isRoot()) {
					if (vq.getoverrideRequiresPrivilegeValue() != null) {
						if (vq.getoverrideRequiresPrivilegeValue()) {
							throw new InformationSchemaException("You do not have permissions to query " + istv.getName().get());
						}
					} else {
						istv.assertPermissions(sc);
					}
				}
				if (istv.isView()) haveViews = true;
				else haveNonViews = true;
			}
		}
		// having checked perms, let's see if the query consists of only info schema views; if so we can just expand and
		// skip all the other stuff.
		if (haveViews && haveNonViews)
			throw new InformationSchemaException("Unable to handle info schema query involving view impls and non view impls");
		if (haveViews) 
			return DirectSchemaQueryEngine.convertDown(sc,vq);
		
		ComputedInformationSchemaTable.derefEntities(in);
		List<ExpressionNode> origProjection = vq.getQuery().getProjection();
		ArrayList<List<InformationSchemaColumn>> columns = new ArrayList<List<InformationSchemaColumn>>();
		for(int i = 0; i < origProjection.size(); i++) {
			ExpressionNode en = origProjection.get(i);
			ExpressionNode targ = ExpressionUtils.getTarget(en);
			ArrayList<InformationSchemaColumn> pathTo = new ArrayList<InformationSchemaColumn>();
			if (targ instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) targ;
				accumulateSelectorPath(pathTo,ci);
			} else if (targ instanceof FunctionCall) {
				pathTo.add(new SyntheticComputedInformationSchemaColumn(new UnqualifiedName("unknown")));
			} else if (targ instanceof LiteralExpression) {
				LiteralExpression le = (LiteralExpression)targ;
				UnqualifiedName columnName = new UnqualifiedName(PEStringUtils.dequote(vq.getQuery().getProjectionMetadata(sc).getColumnInfo(i+1).getAlias()));
				Type type = null;
				try {
					type = BasicType.buildFromLiteralExpression(sc, le);
				} catch (PEException e) {
					throw new InformationSchemaException("Cannot create a proper type for literal value '" + le.getValue(sc) + "'");
				}
				pathTo.add(new ConstantComputedInformationSchemaColumn(null, columnName, type, le.getValue(sc)));
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

		LogicalCatalogQuery lq = new LogicalCatalogQuery(vq, out, vq.getParams(), lca.getForwarding(), columns);
		
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
		LogicalCatalogQuery working = lq;
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
				InformationSchemaColumn icol = (InformationSchemaColumn) sci.getColumn();
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
				ComputedInformationSchemaTable tab = (ComputedInformationSchemaTable) ti.getTable();
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
			InformationSchemaColumn icol = (InformationSchemaColumn) ici.getColumn();
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
				SyntheticComputedInformationSchemaColumn synth = (SyntheticComputedInformationSchemaColumn) icol;
				return synth.buildReplacement(in);
			} else {
				throw new InformationSchemaException("Invalid info schema view column type: " + icol);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private static IntermediateResultSet buildRawResultSet(SchemaContext sc, LogicalCatalogQuery lq, ProjectionInfo pi) {
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
		ColumnSet cs = lq.buildProjectionMetadata(sc, pi, Functional.toList(exampleData.values())); 
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
	
	public static ColumnSet buildProjectionMetadata(SchemaContext sc, List<List<InformationSchemaColumn>> projColumns,
			ProjectionInfo pi) {
		return LogicalCatalogQuery.buildProjectionMetadata(sc, projColumns, pi, null);
	}
	

	/**
	 * @param cs
	 * @param colName
	 * @param colAlias
	 * @param in
	 * @throws PEException
	 */
	
	private static void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in) {
		for(TableKey tk : in.getDerivedInfo().getLocalTableKeys()) {
			InformationSchemaTable istv = (InformationSchemaTable) tk.getTable();
			istv.annotate(sc, vq, in, tk);
		}
		for(ProjectingStatement ss : in.getDerivedInfo().getAllNestedQueries()) {
			if (ss instanceof UnionStatement) continue;
			annotate(sc, vq, (SelectStatement)ss);
		}
	}
	
}
