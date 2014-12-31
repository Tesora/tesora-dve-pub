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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.*;
import org.apache.log4j.Logger;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.direct.DirectInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.direct.DirectVariablesTable;
import com.tesora.dve.sql.infoschema.direct.ViewInformationSchemaTable;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.execution.DDLQueryExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.InformationSchemaRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.ViewRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.NonDMLFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.util.ListSet;

// responsible for flattening view queries down to logical table queries
// and logical table queries down to either hql queries or sql queries.
public class LogicalSchemaQueryEngine {

	static final boolean emit = Boolean.getBoolean("parser.debug");
	private static final Logger logger = Logger.getLogger(LogicalSchemaQueryEngine.class);
	
	private static boolean canLog() { return emit || logger.isDebugEnabled(); }
	
	public static void log(String in) {
		if (emit)
			System.out.println(in);
		if (logger.isDebugEnabled())
			logger.debug(in);
	}
	
	// planning entry point
	public static FeatureStep execute(SchemaContext sc, SelectStatement ss, FeaturePlanner planner) {
		ProjectionInfo pi = ss.getProjectionMetadata(sc);
		ViewQuery vq = new ViewQuery(ss, null, null);
		DirectVariablesTable haveVariables = null;
		boolean haveOthers = false;
		for(TableKey tk : ss.getAllTableKeys()) {
			InformationSchemaTable istv = (InformationSchemaTable) tk.getTable();
			if (istv.isVariablesTable())
				haveVariables = (DirectVariablesTable) istv;
			else
				haveOthers = true;
		}
		if (haveVariables == null) {
		
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
		} else if (haveVariables != null && haveOthers) {
			throw new InformationSchemaException("Illegal information schema query: across variables table and others");
		} else {
			// all variables
			final Statement planned =
					DirectVariablesTable.execute(sc, haveVariables, haveVariables.getDefaultScope(), vq, pi);
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
		LogicalQuery lq = convertDown(sc,vq,planner);
		if (planner == null)
			throw new InformationSchemaException("Missing feature planner");
		return new InfoPlanningResults(buildStep(sc, lq, planner, pi));
	}
	
	public static LogicalQuery convertDown(SchemaContext sc, ViewQuery vq, FeaturePlanner fp) {
		SelectStatement in = vq.getQuery();
		if (canLog()) {
			String sql = in.getSQL(sc);
			log("info schema query before logical conversion: '" + sql + "'");						
		}
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
			}
		}
		return convertDirectDown(sc,vq, fp);
		
	}
	
	public static ColumnSet buildProjectionMetadata(SchemaContext sc, List<List<InformationSchemaColumn>> projColumns,
			ProjectionInfo pi) {
		return buildProjectionMetadata(sc, projColumns, pi, null);
	}
	
	public static ColumnSet buildProjectionMetadata(SchemaContext sc, List<List<InformationSchemaColumn>> projectionColumns,
			ProjectionInfo pi, List<Object> examples) {
		ColumnSet cs = new ColumnSet();
		try {
			for(int i = 0; i < projectionColumns.size(); i++) {
				List<InformationSchemaColumn> p = projectionColumns.get(i);
				InformationSchemaColumn typeColumn = p.get(p.size() - 1);
				InformationSchemaColumn nameColumn = p.get(0);
				Type type = typeColumn.getType();
				if (type == null) {
					Object help = null;
					if (i < examples.size())
						help = examples.get(i);
					if (help == null)
						help = "help";
					ColumnInfo ci = pi.getColumnInfo(i+1);
					LogicalQuery.buildNativeType(cs,ci.getName(),ci.getAlias(),help);					
				} else {
                    NativeType nt = Singletons.require(DBNative.class).getTypeCatalog().findType(type.getDataType(), true);
                	ColumnMetadata cmd = new ColumnMetadata();
                    if (nameColumn.getTable() != null && nameColumn.getTable().getView() == InfoView.INFORMATION) {
                    	cmd.setDbName(nameColumn.getTable().getDatabase(sc).getName().getUnquotedName().get());
                    	cmd.setTableName(nameColumn.getTable().getName().getUnquotedName().get());
                    }
                    if (pi != null)
                    	cmd.setAliasName(pi.getColumnAlias(i+1));
                    cmd.setName(nameColumn.getName().getSQL());
                    cmd.setSize(type.getSize());
                    cmd.setTypeName(nt.getTypeName());
                    cmd.setDataType(type.getDataType());
                    cs.addColumn(cmd);
				}
			}
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to build metadata for catalog result set",pe);
		}
		if (LogicalSchemaQueryEngine.emit) {
			StringBuffer buf = new StringBuffer();
			for(int i = 1; i <= cs.size(); i++) {
				if (i > 1)
					buf.append(", ");
				// buf.append(cs.getColumn(i).getName());
				buf.append(cs.getColumn(i));
			}
			System.out.println("column set: " + buf.toString());
		}
		return cs;
		
	}

	// convenience
	public static FeatureStep buildStep(SchemaContext sc, ViewQuery vq, ProjectionInfo pi) {
		LogicalQuery dlq = convertDown(sc,vq,null);
		return buildStep(sc,dlq,new InformationSchemaRewriteTransformFactory(),pi);
	}
	
	
	public static FeatureStep buildStep(SchemaContext sc, LogicalQuery lq, FeaturePlanner planner, final ProjectionInfo pi) {
		SelectStatement toExecute = lq.getQuery();
		final PEDatabase catalogSchema = Singletons.require(InformationSchemaService.class).getCatalogSchema();
		final GenericSQLCommand gsql = toExecute.getGenericSQL(sc, Singletons.require(DBNative.class).getEmitter(), EmitOptions.NONE);
				
		// look up the system group now - we have to use the actual item
		final PEPersistentGroup sg = sc.findStorageGroup(new UnqualifiedName(PEConstants.SYSTEM_GROUP_NAME));
		
		if (canLog()) {
			log("execute on " + sg.getName().getSQL());
			List<String> lines = new ArrayList<String>();
			gsql.resolveAsTextLines(sc.getValues(), false, "  ", lines);
			for(String s : lines)
				log(s);
			if (pi != null) {
				for(int i = 1; i <= pi.getWidth(); i++) {
					log("[" + i + "]: " + pi.getColumnInfo(i));
				}
			} else {
				log("no projection info");
			}
		}

		return new ProjectingFeatureStep(null, planner, toExecute, new ExecutionCost(true, true, null, -1),
				sg, null, catalogSchema, DistributionVector.buildDistributionVector(sc, Model.BROADCAST, null, null)) {
			
			@Override
			public void scheduleSelf(PlannerContext pc, ExecutionSequence es)
					throws PEException {
				
				ProjectingExecutionStep pes =
						ProjectingExecutionStep.build(
								catalogSchema, sg,
							gsql);
				if (pi != null)
					pes.setProjectionOverride(pi);
				es.append(pes);		
			}

		}.withDefangInvariants();
	}
	
	public static LogicalQuery convertDirectDown(SchemaContext sc, ViewQuery vq, FeaturePlanner fp) {
		SelectStatement in = vq.getQuery();
		SelectStatement copy = CopyVisitor.copy(in);
		if (canLog())
			log("Before conversion: " + copy.getSQL(sc));		
		Converter forwarder = new Converter(sc);
		forwarder.traverse(copy);
		for(Map.Entry<TableKey, TableKey> me : forwarder.getForwardedTableKeys().entrySet()) {
			copy.getDerivedInfo().removeLocalTable(me.getKey().getTable());
			copy.getDerivedInfo().addLocalTable(me.getValue());
		}
		if (canLog())
			log("After conversion: " + copy.getSQL(sc));
		// and now we can explode it
		ViewRewriteTransformFactory.applyViewRewrites(sc, copy, fp);
		Map<String,Object> params = vq.getParams();
		if (params == null) params = new HashMap<String,Object>();
		Long anyTenant = sc.getPolicyContext().getTenantID(false);
		if (sc.getPolicyContext().isContainerContext()) {
			if (anyTenant == null) anyTenant = -1L; // global tenant
		}
		params.put(ViewInformationSchemaTable.tenantVariable,anyTenant);
		params.put(ViewInformationSchemaTable.sessid,new Long(sc.getConnection().getConnectionId()));
		new VariableConverter(params).traverse(copy);
		if (canLog())
			log("After explode: "+ copy.getSQL(sc));
		return new LogicalQuery(vq,copy,Collections.<String, Object> emptyMap());
	}

	private static class Converter extends Traversal {
		
		final SchemaContext context;
		final Map<TableKey,TableKey> forwarding;
		
		public Converter(SchemaContext sc) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			this.context = sc;
			this.forwarding = new HashMap<TableKey,TableKey>();
		}

		public Map<TableKey,TableKey> getForwardedTableKeys() {
			return forwarding;
		}

		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof TableInstance) {
				return map((TableInstance)in);
			} else if (in instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) in;
				if (ci.getColumn() instanceof DirectInformationSchemaColumn) {
					DirectInformationSchemaColumn vc = (DirectInformationSchemaColumn) ci.getColumn();
					PEColumn backing = vc.getColumn();
					return new ColumnInstance(ci.getSpecifiedAs(),backing,map(ci.getTableInstance()));
				}
			}
			return in;
		}
		
		private TableInstance map(TableInstance in) {
			if (in.getTable().isInfoSchema()) {
				// I already know these will be view based info schema tables
				ViewInformationSchemaTable infoTab = (ViewInformationSchemaTable) in.getTable();
				TableInstance out =  new TableInstance(infoTab.getBackingView(),in.getSpecifiedAs(context),in.getAlias(),in.getNode(),false);
				if (!forwarding.containsKey(in.getTableKey()))
					forwarding.put(in.getTableKey(),out.getTableKey());
				return out;
			}
			return in;
		}

	}

	private static class VariableConverter extends Traversal {
		
		final Map<String,Object> params;
		
		public VariableConverter(Map<String,Object> params) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			this.params = params;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof VariableInstance) {
				VariableInstance vi = (VariableInstance) in;
				String name = vi.getVariableName().getUnquotedName().get();
				Object repl = params.get(name);
				if (repl == null)
					return LiteralExpression.makeNullLiteral();
				if (repl instanceof String)
					return LiteralExpression.makeStringLiteral((String)repl);
				else
					return LiteralExpression.makeAutoIncrLiteral((Long)repl);
			}
			return in;
		}
	}
	

	
}
