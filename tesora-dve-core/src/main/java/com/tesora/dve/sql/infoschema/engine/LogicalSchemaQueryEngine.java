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

import java.util.List;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.InformationSchemaTable;
import com.tesora.dve.sql.infoschema.direct.AbstractDirectVariablesTable;
import com.tesora.dve.sql.infoschema.direct.DirectSchemaQueryEngine;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.execution.DDLQueryExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.NonDMLFeatureStep;
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
		ViewQuery vq = new ViewQuery(ss, null, null);
		annotate(sc,vq,ss);
		AbstractDirectVariablesTable haveVariables = null;
		boolean haveOthers = false;
		for(TableKey tk : ss.getAllTableKeys()) {
			InformationSchemaTable istv = (InformationSchemaTable) tk.getTable();
			if (istv.isVariablesTable())
				haveVariables = (AbstractDirectVariablesTable) istv;
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
					AbstractDirectVariablesTable.execute(sc, haveVariables, haveVariables.getDefaultScope(), vq, pi);
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
		if (planner == null)
			throw new InformationSchemaException("Missing feature planner");
		return new InfoPlanningResults(DirectSchemaQueryEngine.buildStep(sc, lq, planner, pi));
	}
	
	// convenience entry point, transitional
	public static IntermediateResultSet buildResultSet(SchemaContext sc, ViewQuery vq, ProjectionInfo pi) {
		InfoPlanningResults ipr = buildResults(sc,vq,pi,null);
		if (ipr.getResults() != null)
			return ipr.getResults();
		throw new InformationSchemaException("No results available (executed via direct)");
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
		
		throw new InformationSchemaException("Unmigrated info schema query");
		
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
                    NativeType nt = Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(type.getDataType(), true);
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
