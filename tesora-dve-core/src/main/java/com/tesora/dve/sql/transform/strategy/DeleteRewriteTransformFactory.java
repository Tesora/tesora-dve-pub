package com.tesora.dve.sql.transform.strategy;

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
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.JoinGraph;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.schema.VectorRange;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.UpdateExecutionSequence;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.MultiFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.join.IndexCollector;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;

/*
 * Applies in any of the following cases:
 * [a] the query is not colocated
 * [b] there is an order by and/or limit clause and any nonbroadcast tables are present
 * 
 * The general strategy is to convert the delete to a select to obtain the appropriate rows,
 * then redist the select back onto the pg and do a delete with a join
 */
public class DeleteRewriteTransformFactory extends TransformFactory {
	
	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.DELETE;
	}

	private boolean applies(DMLStatement stmt, PlannerContext context) throws PEException {
		DeleteStatement sp = (DeleteStatement) stmt;

		PEForeignKey.doForeignKeyChecks(context.getContext(), sp);
		
		// if NOT MT and all the referenced tables are broadcast then we can
		// apply this transform
		boolean isAllTablesBroadcast = true;
		for(TableKey tbl : EngineConstant.TABLES_INC_NESTED.getValue(sp, context.getContext())) {
			if (!tbl.getAbstractTable()
					.getDistributionVector(context.getContext())
					.isBroadcast()) {
				// not a broadcast table
				isAllTablesBroadcast = false;
			}
		}
		if (!isAllTablesBroadcast && (sp.getOrderBys().size() > 0 || sp.getLimit() != null)) {
			return true;
		}
		
		if (EngineConstant.NESTED.hasValue(stmt,context.getContext())) 
			return true;
		JoinGraph jg = EngineConstant.PARTITIONS.getValue(stmt, context.getContext());
		return jg.requiresRedistribution();		
	}
	
	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!applies(stmt,ipc))
			return null;

		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		SchemaContext sc = context.getContext();
		DeleteStatement ds = (DeleteStatement) stmt;
		DeleteStatement copy = CopyVisitor.copy(ds);
		MultiMap<TableInstance,Column<?>> requiredColumns = new MultiMap<TableInstance,Column<?>>();
		List<TableInstance> deleteFromTables = copy.getTargetDeletes();
		for(TableInstance ti : deleteFromTables) {
			TableKey tk = ti.getTableKey();
			PEAbstractTable<?> tab = tk.getAbstractTable();
			if (tab.isView())
				throw new SchemaException(Pass.PLANNER, "No support for deleting from views");
			ListSet<Column<?>> ucolumns = new ListSet<Column<?>>();
			// there's no point in projecting the dist key if there is more than one target table
			if (deleteFromTables.size() == 1 && tab.getDistributionVector(sc).usesColumns(sc)) 
				ucolumns.addAll(tab.getDistributionVector(sc).getColumns(sc));
			PEKey key = tab.asTable().getPrimaryKey(sc);
			if (key != null) 
				ucolumns.addAll(key.getColumns(sc));
			else {
				// see if we have a unique key
				PEKey unique = null;
				for(PEKey pek : tab.getKeys(sc)) {
					if (pek.isUnique()) {
						unique = pek;
						break;
					}
				}
				if (unique != null) {
					ucolumns.addAll(unique.getColumns(sc));
				}
			}
			if (ucolumns.isEmpty()) {
				// we need unique columns in order to build the join
				throw new PEException("Degenerate delete case: no dist key and no primary key");
			}
			requiredColumns.putAll(ti,ucolumns);
		}
		SelectStatement ss = DMLStatementUtils.convertDeleteToSelect(copy,requiredColumns);
		ss.normalize(sc);
		
		ProjectingFeatureStep child = (ProjectingFeatureStep) buildPlan(ss,context,DefaultFeaturePlannerFilter.INSTANCE);

		boolean needOrderByLimit = ((copy.getLimit() != null) || 
				(copy.getOrderBys().size() > 0));

		SelectStatement childSS = (SelectStatement) child.getPlannedStatement();
		if (needOrderByLimit) {
			childSS.setOrderBy(childSS.getMapper().copyForward(copy.getOrderBys()));
			childSS.setLimit(copy.getLimit());
		}

		List<TableInstance> tabs = Functional.toList(requiredColumns.keySet());
		RedistFeatureStep redistributed = null;
		Model model = null;
		if (tabs.size() > 1)
			model = Model.BROADCAST;
		else {
			TableInstance ti = tabs.get(0);
			PEAbstractTable<?> pet = ti.getAbstractTable();
			model = (pet.getDistributionVector(sc).usesColumns(sc)? Model.STATIC : Model.BROADCAST);
		}
		if (model == Model.BROADCAST) {
			// we need as many temp tables as there are distinct storage groups
			ListSet<PEStorageGroup> sgs = new ListSet<PEStorageGroup>();
			if (needOrderByLimit) {
				sgs.add(context.getTempGroupManager().getGroup(true));
			} else {
				for(TableInstance ti : tabs)
					sgs.add(ti.getAbstractTable().getStorageGroup(sc));
			}

			if (sgs.size() > 1)
				throw new SchemaException(Pass.PLANNER, "Unable to correctly plan lookup table for multitable delete where tables not in same storage group");

			redistributed =
					child.redist(context, this,
							new TempTableCreateOptions(model,sgs.get(0))
								.withRowCount(-1),
							null,
							null);
		} else {
			// dist vector columns are leading columns in select
			// we need to arrange for the aliases on the projection to match the 
			// columns in the dist vect
			PEAbstractTable<?> pet = tabs.get(0).getAbstractTable();
			ArrayList<Integer> dvect = new ArrayList<Integer>();
			List<PEColumn> dvectCols = pet.getDistributionVector(sc).getColumns(sc);
			for(int i = 0; i < dvectCols.size(); i++) {
				ExpressionNode projEntry = childSS.getProjectionEdge().get(i);
				ExpressionAlias repl = null;
				if (projEntry instanceof ExpressionAlias) {
					repl = (ExpressionAlias) projEntry;
					repl.setAlias(dvectCols.get(i).getName().getUnqualified());
				} else {
					Edge<?,ExpressionNode> parentEdge = projEntry.getParentEdge();
					repl = new ExpressionAlias(projEntry,new NameAlias(dvectCols.get(i).getName().getUnqualified()),null);
					parentEdge.set(repl);
				}
				dvect.add(i);
			}

			List<Integer> dvColOffsets = dvect;
			model = pet.getDistributionVector(sc).getModel();
			VectorRange rangeDist = pet.getDistributionVector(sc).getRangeDistribution();
			PEStorageGroup sg = pet.getPersistentStorage(sc);
			if (needOrderByLimit) {
				dvColOffsets = Collections.emptyList();
				model = Model.STATIC;
				rangeDist = null;
				sg = context.getTempGroupManager().getGroup(true);
				pet = null; // do not use this table's range
			}
			redistributed =
					child.redist(context, this,
							new TempTableCreateOptions(model,sg)
								.withRange(rangeDist)
								.withRowCount(-1)
								.distributeOn(dvColOffsets),
							null,
							null);
		}			

		if (needOrderByLimit) {
			ProjectingFeatureStep pfs = redistributed.buildNewProjectingStep(context, this, null, null);
			// add the order by and limit clauses from the delete to the select

			SelectStatement tss = (SelectStatement) pfs.getPlannedStatement();
			tss.setOrderBy(tss.getMapper().copyForward(copy.getOrderBys()));
			tss.setLimit(tss.getMapper().copyForward(copy.getLimit()));

			ProjectingFeatureStep plannedPFS = 
					(ProjectingFeatureStep) buildPlan(tss,context,DefaultFeaturePlannerFilter.INSTANCE);
			
			plannedPFS.prefixChildren(Collections.singletonList((FeatureStep)redistributed));
			
			PEAbstractTable<?> table = requiredColumns.keySet().iterator().next().getAbstractTable();
			
			// build the select with order by limit on the supplied temp table
			// make the redist broadcast
			redistributed = plannedPFS.redist(context, this,
					new TempTableCreateOptions(Model.BROADCAST, table.getPersistentStorage(sc))
						.withRowCount(-1),
						null,
						null);
		}

		
		
		List<DeleteStatement> out = new ArrayList<DeleteStatement>();
		int offset = 0;
		for(TableInstance ti : requiredColumns.keySet()) {
			List<ExpressionNode> equijoins = new ArrayList<ExpressionNode>();
			SelectStatement ctts = redistributed.buildNewSelect(context);
			List<Column<?>> cols = (List<Column<?>>) requiredColumns.get(ti);
			for(int i = 0; i < cols.size(); i++) {
				ColumnInstance lhs = new ColumnInstance(cols.get(i), ti);
				ExpressionNode rhs = ExpressionUtils.getTarget(ctts.getProjectionEdge().get(i + offset));
				FunctionCall eq = new FunctionCall(FunctionName.makeEquals(),lhs,rhs);
				equijoins.add(eq);
			}
			offset += cols.size();
			ExpressionNode joinEx = null;
			if (equijoins.size() == 1)
				joinEx = equijoins.get(0);
			else
				joinEx = ExpressionUtils.buildAnd(equijoins);
			// now build delete ti from ti inner join ...
			TableInstance tti = ctts.getBaseTables().get(0);
			JoinedTable jt = new JoinedTable(tti, joinEx, JoinSpecification.INNER_JOIN);
			FromTableReference ftr = new FromTableReference(new TableJoin(ti, Collections.singletonList(jt)));
			DeleteStatement cds = new DeleteStatement(Collections.singletonList(ftr),null,false,null);
			cds.getDerivedInfo().addLocalTable(ti.getTableKey(),tti.getTableKey());
			cds.normalize(sc);
			out.add(cds);
			IndexCollector.collect(sc, joinEx);
		}

		if (out.size() == 1) {
			FeatureStep result = DefaultFeatureStepBuilder.INSTANCE.buildStep(context, this, out.get(0), null, DMLExplainReason.SINGLE_TABLE_DELETE.makeRecord());
			result.addChild(redistributed);
			return result;
		} else {
		
			MultiFeatureStep result = new MultiFeatureStep(this) {

				@Override
				public void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
					if (!scheduled.add(this)) return;
					schedulePrefix(sc,es,scheduled);
					getSelfChildren().get(0).schedule(sc,es,scheduled);
					// we only want the updates to be part of the update exec sequence
					UpdateExecutionSequence ues = new UpdateExecutionSequence(es.getPlan());
					es.append(ues);
					for(int i = 1; i < getSelfChildren().size(); i++) {
						getSelfChildren().get(i).schedule(sc,ues,scheduled);
					}
				}

			};
			result.withDefangInvariants();
			result.addChild(redistributed);
			for(DeleteStatement sds : out) {
				result.addChild(DefaultFeatureStepBuilder.INSTANCE.buildStep(context, this, sds, null,
						DMLExplainReason.MULTI_TABLE_DELETE.makeRecord()));
			}

			return result;
			
		}
		
	}

}
