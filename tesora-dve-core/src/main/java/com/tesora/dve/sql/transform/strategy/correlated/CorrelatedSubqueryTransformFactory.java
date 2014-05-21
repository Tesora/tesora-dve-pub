package com.tesora.dve.sql.transform.strategy.correlated;

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
import java.util.Collection;
import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.SingleSiteStorageGroupTransformFactory;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.nested.NestingStrategy;
import com.tesora.dve.sql.transform.strategy.nested.NestingStrategy.ScalarCheckResult;
import com.tesora.dve.sql.util.ListSet;

abstract class CorrelatedSubqueryTransformFactory extends TransformFactory {

	private static final EdgeTest[] supported =
			new EdgeTest[] { EngineConstant.WHERECLAUSE, EngineConstant.PROJECTION };
	
	protected abstract EdgeTest getMatchLocation();

	protected abstract FeatureStep buildSteps(PlannerContext pc,List<CorrelatedSubquery> subs,
			SelectStatement origQuery,SelectStatement workingCopy) throws PEException;
	
	protected abstract CorrelatedSubquery buildSubquery(SchemaContext sc, ProjectingStatement subq,
			SelectStatement enclosing,
			ListSet<ColumnKey> outerColumns)
		throws PEException;
	
	protected boolean applies(SchemaContext sc, final DMLStatement stmt) throws PEException {
		if (SingleSiteStorageGroupTransformFactory.isSingleSite(sc, stmt,true)) return false;
		// we apply when there are nested queries and they have tables from the parent
		final ListSet<ProjectingStatement> correlated = new ListSet<ProjectingStatement>();
		findCorrelated(sc, stmt, new SubqueryCollector() {

			@Override
			public void onCorrelatedSubquery(SchemaContext sc, ProjectingStatement subq,
					ListSet<ColumnKey> outerColumns,
					ListSet<ColumnKey> innerColumns) throws PEException {
				ExpressionPath ep = ExpressionPath.build(subq,stmt);
				// we only support scalar results right now
				final ScalarCheckResult scalarCheck = NestingStrategy.hasScalarResult(sc, subq);
				if (!innerColumns.isEmpty() && !scalarCheck.isValid())
					throw new PEException("Unsupported correlated subquery - result not scalar (" + scalarCheck.getDescribtion() + ")");
				EdgeTest which = null;
				for(EdgeTest et : supported)
					if (ep.has(et)) {
						which = et;
						break;
					}
				if (which == null)
					throw new PEException("Unsupported correlated subquery - unknown location");
				if (which == getMatchLocation())
					correlated.add(subq);
			}
			
		});
		return !correlated.isEmpty();		
	}
	
	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context)
			throws PEException {
		if (!applies(context.getContext(),stmt))
			return null;
		return buildStep(context,stmt);
	}

	
	protected static void findCorrelated(SchemaContext sc, DMLStatement stmt, SubqueryCollector coll) throws PEException {
		ListSet<ProjectingStatement> nested = stmt.getDerivedInfo().getLocalNestedQueries();
		ListSet<TableKey> enclosingTabs = EngineConstant.TABLES.getValue(stmt, sc);
		
		for(ProjectingStatement ps : nested) {
			ProjectingStatement parent = ps.getParent().getEnclosing(ProjectingStatement.class,  null);
			// one level at a time
			if (parent != stmt) continue;
			ListSet<ColumnKey> correlatedOuterColumns = new ListSet<ColumnKey>();
			ListSet<ColumnKey> innerColumns = new ListSet<ColumnKey>();
			for(ColumnInstance ci : ColumnInstanceCollector.getColumnInstances(ps)) {
				ColumnKey ck = ci.getColumnKey();
				if (enclosingTabs.contains(ck.getTableKey())) {
					correlatedOuterColumns.add(ck);
				} else {
					innerColumns.add(ck);
				}
			}
			if (correlatedOuterColumns.isEmpty()) 
				continue;
			coll.onCorrelatedSubquery(sc, ps, correlatedOuterColumns,innerColumns);
		}
	}
	
	interface SubqueryCollector {
		
		// return true if we're done
		public void onCorrelatedSubquery(SchemaContext sc, ProjectingStatement subq, ListSet<ColumnKey> outerColumns, ListSet<ColumnKey> innerColumns) throws PEException;
		
	}
	
	private boolean orderQueries(SchemaContext sc, final SelectStatement copy, final MultiMap<Integer,CorrelatedSubquery> corsubs, 
			final ListSet<ProjectingStatement> notActuallyCorrelated) throws PEException {
		findCorrelated(sc, copy, new SubqueryCollector() {

			@Override
			public void onCorrelatedSubquery(SchemaContext sc, ProjectingStatement subq,
					ListSet<ColumnKey> outerColumns,
					ListSet<ColumnKey> innerColumns) throws PEException {
				if (innerColumns.isEmpty()) {
					notActuallyCorrelated.add(subq);
					return;
				}
				final ScalarCheckResult scalarCheck = NestingStrategy.hasScalarResult(sc, subq);
				if (!scalarCheck.isValid())
					throw new PEException("Unsupported correlated subquery - result not scalar (" + scalarCheck.getDescribtion() + ")");
				ExpressionPath ep = ExpressionPath.build(subq,copy);
				if (ep.has(getMatchLocation())) {
					CorrelatedSubquery csq = buildSubquery(sc,subq,copy,outerColumns);
					corsubs.put(csq.getOffset(), csq);
				}				
			}
			
		});
		boolean any = false;
		for(CorrelatedSubquery c : corsubs.values()) {
			if (c.simplify(sc,this))
				any = true;
		}
		return any;
	}
	
		
	protected FeatureStep buildStep(PlannerContext ipc, DMLStatement stmt) throws PEException {
		SelectStatement ss = (SelectStatement) stmt;
		final SelectStatement copy = (SelectStatement) CopyVisitor.copy(stmt);

		PlannerContext pc = ipc.withTransform(getFeaturePlannerID());
		
		if (emitting())
			emit("in " + copy.getSQL(pc.getContext(), "  "));
		
		ListSet<ProjectingStatement> notActuallyCorrelated = new ListSet<ProjectingStatement>();
		MultiMap<Integer,CorrelatedSubquery> corsubs = 
				new MultiMap<Integer,CorrelatedSubquery>(new MultiMap.OrderedMapFactory<Integer,CorrelatedSubquery>());
		while(orderQueries(pc.getContext(), copy, corsubs, notActuallyCorrelated)) {
			corsubs.clear();
			notActuallyCorrelated.clear();
		}
		
		// if not actually correlated, rewrite in the usual way
		for(ProjectingStatement ps : notActuallyCorrelated) {
			if (ps.getProjections().get(0).size() > 1)
				throw new PEException("Invalid noncorrelated subquery - returns more than one column");
			Subquery sq = ps.getParent().getEnclosing(Subquery.class, ProjectingStatement.class);
			if (sq == null)
				throw new PEException("Malformed statement - nested query not a subquery");
			sq.getParentEdge().set(ps.getProjections().get(0).get(0));
		}
		
		// build the ordered list of correlated subqueries
		ListSet<CorrelatedSubquery> subq = new ListSet<CorrelatedSubquery>();
		for(Integer i : corsubs.keySet()) {
			Collection<CorrelatedSubquery> csubqs = corsubs.get(i);
			subq.addAll(csubqs);
		}

		if (!subq.isEmpty()) {
			PlannerContext subcontext = pc.withCosting();
			return buildSteps(subcontext,subq,ss,copy);
		} else {
			return buildPlan(copy,pc,DefaultFeaturePlannerFilter.INSTANCE);
		}
	}
	
	
	protected RedistFeatureStep redistToAggSite(PlannerContext pc, ProjectingFeatureStep srcStep) throws PEException {
		return srcStep.redist(pc, this,
				new TempTableCreateOptions(Model.STATIC,pc.getTempGroupManager().getGroup(true))
					.withRowCount(srcStep.getCost().getRowCount()),
				null, 
				null);
	}

	protected RedistFeatureStep buildLookupTable(PlannerContext pc, RedistFeatureStep srcTab, 
			ListSet<ColumnKey> allOuterColumns, PEStorageGroup group) 
			throws PEException {
		ProjectingFeatureStep pfs = srcTab.buildNewProjectingStep(pc, this, null, null);
		SelectStatement q = (SelectStatement) pfs.getPlannedStatement();
		List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		for(ColumnKey ck : allOuterColumns) {
			ColumnKey tck = q.getMapper().copyColumnKeyForward(ck);
			proj.add(tck.toInstance());
		}
		q.setProjection(proj);
		q.normalize(pc.getContext());
		RedistFeatureStep out =
				pfs.redist(pc, this, 
						new TempTableCreateOptions(Model.BROADCAST,group)
							.withRowCount(srcTab.getTargetTempTable().getTableSizeEstimate(pc.getContext())),
						null, 
						DMLExplainReason.CORRELATED_SUBQUERY_LOOKUP_TABLE.makeRecord());
		TempTable tt = out.getTargetTempTable();
		tt.noteJoinedColumns(pc.getContext(), tt.getColumns(pc.getContext()));
		return out;
	}

	
}
