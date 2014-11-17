package com.tesora.dve.sql.transform.strategy.triggers;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.TriggerValueHandler;
import com.tesora.dve.queryplan.TriggerValueHandlers;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TriggerTableKey;
import com.tesora.dve.sql.node.expression.AutoIncrementLiteralExpression;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LateBindingConstantExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.node.expression.TriggerTableInstance;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETableTriggerPlanningEventInfo;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.schema.TriggerTime;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.modifiers.ColumnKeyModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifierKind;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.CreateTempTableExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.HasPlanning;
import com.tesora.dve.sql.transform.execution.TriggerExecutionStep;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.insert.InsertIntoValuesPlanner;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public class InsertIntoValuesTriggerPlanner extends TriggerPlanner {

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context)
			throws PEException {
		
		if (!(stmt instanceof InsertIntoValuesStatement)) 
			return null;
		
		InsertIntoValuesStatement iivs = (InsertIntoValuesStatement) stmt;

		PETable intoTable = iivs.getPrimaryTable().getAbstractTable().asTable();
		
		PETableTriggerPlanningEventInfo triggerInfo = (PETableTriggerPlanningEventInfo) 
				intoTable.getTriggers(context.getContext(), TriggerEvent.INSERT);
		
		if (triggerInfo == null)
			return null;
		
		List<ColumnKey> rowTableColumnOrder = buildNormalizedOrder(context,intoTable,triggerInfo);

		List<TriggerValueHandler> valueHandlers = Functional.apply(rowTableColumnOrder, new UnaryFunction<TriggerValueHandler,ColumnKey>() {

			@Override
			public TriggerValueHandler evaluate(ColumnKey object) {
				if (object.getPEColumn().isAutoIncrement()) {
					return new AutoincrementTriggerValueHandler(object.getPEColumn());
				} else {
					return new TriggerValueHandler(object.getPEColumn().getType());
				}
			}
			
		});
		
		TempTable rowsTable = buildRowsTable(context, intoTable, triggerInfo, rowTableColumnOrder);

		SelectStatement intent = rowsTable.buildSelect(context.getContext());
		ProjectingFeatureStep rowsQuery = 
				DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(context, this, 
						intent,
						new ExecutionCost(false,true,null,iivs.getValues().size()),
						rowsTable.getStorageGroup(context.getContext()),
						intoTable.getPEDatabase(context.getContext()),
						rowsTable.getDistributionVector(context.getContext()),
						null, 
						null);

		
		InsertIntoValuesStatement populateInsert = buildPopulateInsert(context, rowsTable, rowTableColumnOrder, iivs);
		
		FeatureStep populateStep = 
				buildPlan(populateInsert,context.withTransform(getFeaturePlannerID()),
						DefaultFeaturePlannerFilter.INSTANCE);

		// now, we're going to build the single row insert
		InsertIntoValuesStatement singleRowInsert = buildOneTupleInsert(context, intoTable, rowTableColumnOrder); 
		
		FeatureStep singleRowStep = 
				InsertIntoValuesPlanner.buildInsertIntoValuesFeatureStep(context,this,singleRowInsert);

		return new InsertIntoValuesTriggerFeatureStep(this,intoTable,rowsQuery,
				new TriggerValueHandlers(valueHandlers.toArray(new TriggerValueHandler[0])),
				rowsTable,populateStep,singleRowStep,
				triggerInfo.getBeforeStep(context.getContext()),
				triggerInfo.getAfterStep(context.getContext()));
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.INSERT_INTO_VALUES_TRIGGER;
	}

	private List<ColumnKey> buildNormalizedOrder(PlannerContext context, PETable theTable, PETableTriggerPlanningEventInfo info) throws PEException {
		
		ArrayList<ColumnKey> out = new ArrayList<ColumnKey>();
		
		Collection<ColumnKey> triggerColumns = info.getTriggerBodyColumns(context.getContext());

		ListSet<PEColumn> usedTriggerColumns = new ListSet<PEColumn>();
		for(ColumnKey ck : triggerColumns)
			usedTriggerColumns.add(ck.getPEColumn());
		
		ListSet<PEColumn> allColumns = new ListSet<PEColumn>(theTable.getColumns(context.getContext()));
		allColumns.removeAll(usedTriggerColumns);

		out.addAll(triggerColumns);
		
		TriggerTableInstance tti = new TriggerTableInstance(theTable,-1,TriggerTime.AFTER);
		for(PEColumn pec : allColumns) {
			out.add(new ColumnInstance(pec,tti).getColumnKey());
		}
		
		return out;
	}
	
	private TempTable buildRowsTable(PlannerContext context, PETable intoTable, 
			PETableTriggerPlanningEventInfo info, List<ColumnKey> columnOrder) throws PEException {
		// convert orig table columns to temp table columns
		List<PEColumn> fields = new ArrayList<PEColumn>();
		for(ColumnKey ck : columnOrder) {
			PEColumn backing = ck.getPEColumn();
			// for this we just do the same type, but make it nullable
			List<ColumnModifier> mods = new ArrayList<ColumnModifier>();
			mods.add(new ColumnModifier(ColumnModifierKind.NULLABLE));
			PEColumn newColumn = PEColumn.buildColumn(context.getContext(),
					new UnqualifiedName("_lbc" + fields.size()),
					backing.getType(),
					mods,
					null,
					Collections.<ColumnKeyModifier> emptyList());
			fields.add(newColumn);
		}
		
		return TempTable.buildAdHoc(context.getContext(), intoTable.getPEDatabase(context.getContext()),
				fields, Model.BROADCAST, Collections.<PEColumn> emptyList(),
				context.getTempGroupManager().getGroup(true),
				true);
	}
	
	private InsertIntoValuesStatement buildPopulateInsert(PlannerContext context, TempTable rowsTable, List<ColumnKey> rowTableColumnOrder, 
			InsertIntoValuesStatement orig) throws PEException {
		
		// existing offsets of columns in the tuples
		HashMap<PEColumn,Integer> tupleOffsetOf = new HashMap<PEColumn,Integer>();
		for(ExpressionNode en : orig.getColumnSpecificationEdge()) {
			ColumnInstance ci = (ColumnInstance) en;
			tupleOffsetOf.put(ci.getPEColumn(), tupleOffsetOf.size());
		}

		final TempTableInstance tti = new TempTableInstance(context.getContext(),rowsTable);
		
		List<ExpressionNode> columnSpec = Functional.apply(rowsTable.getColumns(context.getContext()), new UnaryFunction<ExpressionNode,PEColumn>() {

			@Override
			public ExpressionNode evaluate(PEColumn object) {
				return new ColumnInstance(object,tti);
			}
			
		});

		List<List<ExpressionNode>> orderedValues = new ArrayList<List<ExpressionNode>>();
		
		for(List<ExpressionNode> tuple : orig.getValues()) {
			ArrayList<ExpressionNode> newTuple = new ArrayList<ExpressionNode>();
			for(ColumnKey ck : rowTableColumnOrder) {
				PEColumn col = ck.getPEColumn();
				ExpressionNode expr = tuple.get(tupleOffsetOf.get(col));
				if (col.isAutoIncrement()) {
					TriggerTableKey ttk = (TriggerTableKey) ck.getTableKey();
					if (ttk.getTime() == TriggerTime.BEFORE) {
						// if the corresponding expr node is not a autoinc literal, toss it in
						// if it is an autoinc literal, put in a null
						if (expr instanceof AutoIncrementLiteralExpression) {
							newTuple.add(LiteralExpression.makeNullLiteral());
						} else {
							newTuple.add(expr);
						}
					} else {
						// whatever the value is
						newTuple.add(expr);
					}
				} else {
					// get the value
					newTuple.add(expr);
				}
			}
			orderedValues.add(newTuple);
		}
		
		InsertIntoValuesStatement iivs = new InsertIntoValuesStatement(tti,columnSpec,orderedValues,
				Collections.<ExpressionNode> emptyList(),new AliasInformation(),null);
		iivs.getDerivedInfo().addLocalTable(tti.getTableKey());
		return iivs;
	}
	
	private InsertIntoValuesStatement buildOneTupleInsert(PlannerContext context, 
			PETable intoTable, List<ColumnKey> rowTableColumnOrder) throws PEException {
		final TableInstance nti = new TableInstance(intoTable,intoTable.getName(),
				null, context.getContext().getNextTable(),false);
		final HashMap<ColumnKey,Integer> rowOffsets = new HashMap<ColumnKey,Integer>();
		for(ColumnKey ck : rowTableColumnOrder) 
			rowOffsets.put(ck,rowOffsets.size());
		
		List<ExpressionNode> colSpec = Functional.apply(rowTableColumnOrder, new UnaryFunction<ExpressionNode,ColumnKey>() {

			@Override
			public ExpressionNode evaluate(ColumnKey object) {
				return new ColumnInstance(object.getPEColumn(),nti);
			}
			 
		});

		List<ExpressionNode> values = Functional.apply(rowTableColumnOrder, new UnaryFunction<ExpressionNode,ColumnKey>() {

			@Override
			public ExpressionNode evaluate(ColumnKey object) {
				return new LateBindingConstantExpression(rowOffsets.get(object),object.getPEColumn().getType());
			}
			
		});
		
		InsertIntoValuesStatement iivs = new InsertIntoValuesStatement(nti,colSpec,Collections.singletonList(values),
				Collections.<ExpressionNode> emptyList(),new AliasInformation(),null);
		iivs.getDerivedInfo().addLocalTable(nti.getTableKey());
		return iivs;
	}
	
	protected static class InsertIntoValuesTriggerFeatureStep extends AbstractTriggerFeatureStep {

		private final TempTable rowsTable;
		private final FeatureStep populateStep;
		private final FeatureStep actualStep;
		private final FeatureStep beforeStep;
		private final FeatureStep afterStep;
		
		public InsertIntoValuesTriggerFeatureStep(FeaturePlanner planner,
				PETable actualTable, FeatureStep rowsQuery, TriggerValueHandlers handlers,
				TempTable rowsTable, FeatureStep populateStep, FeatureStep actualStep,
				FeatureStep beforeStep, FeatureStep afterStep) {
			super(planner, actualTable, rowsQuery, handlers);
			this.rowsTable = rowsTable;
			this.populateStep = populateStep;
			this.actualStep = actualStep;
			this.beforeStep = beforeStep;
			this.afterStep = afterStep;
			// make sure the traversal still works
			addChild(populateStep);
			addChild(actualStep);
			if (beforeStep != null)
				addChild(beforeStep);
			if (afterStep != null)
				addChild(afterStep);
		}

		@Override
		public void schedule(PlannerContext sc, ExecutionSequence es,
				Set<FeatureStep> scheduled) throws PEException {
			// TODO Auto-generated method stub
			es.append(new CreateTempTableExecutionStep(rowsTable.getPEDatabase(sc.getContext()),rowsTable.getStorageGroup(sc.getContext()),rowsTable));
			populateStep.schedule(sc,es,scheduled);
			TriggerExecutionStep step = new TriggerExecutionStep(onTable.getPEDatabase(sc.getContext()),
					onTable.getStorageGroup(sc.getContext()),
					(ExecutionStep)buildSubSequence(sc,actualStep,es.getPlan()),
					(beforeStep == null ? null : buildSubSequence(sc,beforeStep,es.getPlan())),
					(afterStep == null ? null : buildSubSequence(sc,afterStep,es.getPlan())),
					(ExecutionStep)buildSubSequence(sc,rowQuery,es.getPlan()),
					handlers);
			es.append(step);
		}
		
	}
}
