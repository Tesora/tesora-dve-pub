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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.AutoIncrementTracker;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.ExecutionState;
import com.tesora.dve.queryplan.TriggerValueHandler;
import com.tesora.dve.queryplan.TriggerValueHandlers;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LateBindingConstantExpression;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETableTriggerPlanningEventInfo;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.TriggerExecutionStep;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.MultiFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistributionFlags;
import com.tesora.dve.sql.util.ListSet;

public abstract class TriggerPlanner extends TransformFactory {

	protected PETableTriggerPlanningEventInfo getTriggerInfo(PlannerContext context, TableKey tk, TriggerEvent event) {
		if (tk.getAbstractTable().isView()) return null;
		PETable pet = tk.getAbstractTable().asTable();
		return (PETableTriggerPlanningEventInfo) pet.getTriggers(context.getContext(), event);
	}
	
	protected ListSet<PETable> getTriggeredTables(PlannerContext context, Collection<TableKey> pertinentTables, TriggerEvent event) {
		ListSet<PETable> triggered = new ListSet<PETable>();
		for(TableKey tk : pertinentTables) {
			if (tk.getAbstractTable().isView()) continue;
			PETable pet = tk.getAbstractTable().asTable();
			if (pet.hasTrigger(context.getContext(), event))
				triggered.add(pet);
		}
		return triggered;
	}
	
	protected FeatureStep failSupport() throws PEException {
		throw new PEException("No planning/runtime support for triggers");
	}
	
	protected FeatureStep handleInsertStatement(PlannerContext context, InsertStatement is) throws PEException {
		TableKey targetTab = is.getTableInstance().getTableKey();
		PETableTriggerPlanningEventInfo triggerInfo = getTriggerInfo(context,targetTab,TriggerEvent.INSERT);
		if (triggerInfo == null)
			return null;
		return failSupport();
	}

	protected ExpressionNode buildUniqueWhereClause(TableKey targetTable, Map<PEColumn,Integer> uniqueKeyOffsets) {
		List<ExpressionNode> eqs = new ArrayList<ExpressionNode>();
		for(Map.Entry<PEColumn,Integer> me : uniqueKeyOffsets.entrySet()) {
			eqs.add(new FunctionCall(FunctionName.makeEquals(),
					new ColumnInstance(me.getKey(),targetTable.toInstance()),
					new LateBindingConstantExpression(me.getValue(),me.getKey().getType())));
		}
		return ExpressionUtils.safeBuildAnd(eqs);
	}
	
	protected BasicTriggerFeatureStep commonPlanning(PlannerContext context, TableKey targetTable, SelectStatement srcSelect, TriggerValueHandlers handlers, 
			DMLStatement uniqueStatement,
			PETableTriggerPlanningEventInfo triggerInfo) throws PEException {
		
		ProjectingFeatureStep srcStep =
				(ProjectingFeatureStep) buildPlan(srcSelect, 
						context.withTransform(getFeaturePlannerID()),
						DefaultFeaturePlannerFilter.INSTANCE);
		
		RedistFeatureStep rowsTable =
				srcStep.redist(context, this,
						new TempTableCreateOptions(Model.BROADCAST,
								context.getTempGroupManager().getGroup(true)),
						new RedistributionFlags(),
						DMLExplainReason.TRIGGER_SRC_TABLE.makeRecord());
		
		ProjectingFeatureStep rows = rowsTable.buildTriggerRowsStep(context, this);
		
		ParserOptions was = context.getContext().getOptions();
		FeatureStep targetStep = null;
		try {
			context.getContext().setOptions(was.setTriggerPlanning());
			targetStep =
					buildPlan(uniqueStatement,context.withTransform(getFeaturePlannerID()),
							DefaultFeaturePlannerFilter.INSTANCE);
		} finally {
			context.getContext().setOptions(was);
		}
				
		return new BasicTriggerFeatureStep(this,targetTable.getAbstractTable().asTable(),
				rowsTable,rows,handlers,targetStep,
				triggerInfo.getBeforeStep(context.getContext()),
				triggerInfo.getAfterStep(context.getContext()));
	}
	
	protected static abstract class AbstractTriggerFeatureStep extends MultiFeatureStep {

		protected final FeatureStep rowQuery;
		protected final PETable onTable;
		protected final TriggerValueHandlers handlers;

		public AbstractTriggerFeatureStep(FeaturePlanner planner, PETable actualTable, FeatureStep rowsQuery, TriggerValueHandlers handlers) {
			super(planner);
			this.rowQuery = rowsQuery;
			this.onTable = actualTable;
			this.handlers = handlers;
			withDefangInvariants();
		}

		protected ExecutionStep buildSubSequence(PlannerContext pc, FeatureStep step, RootExecutionPlan parentPlan) throws PEException {
			ExecutionSequence sub = new ExecutionSequence(parentPlan);
			step.schedule(pc,sub,new HashSet<FeatureStep>());
			if (sub.getSteps().size() == 1)
				return (ExecutionStep)sub.getSteps().get(0);
			return sub;
		}

		@Override
		public abstract void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException;

	}
	
	protected static class BasicTriggerFeatureStep extends AbstractTriggerFeatureStep {

		private final RedistFeatureStep rowsTable;
		private final FeatureStep actual;
		private final FeatureStep before;
		private final FeatureStep after;
		
		public BasicTriggerFeatureStep(FeaturePlanner planner, PETable actualTable, RedistFeatureStep rowsTable, FeatureStep rowsQuery,
				TriggerValueHandlers handlers,
				FeatureStep actual, FeatureStep before, FeatureStep after) {
			super(planner,actualTable,rowsQuery,handlers);
			this.rowsTable = rowsTable;
			this.actual = actual;
			this.before = before;
			this.after = after;
			// make sure the traversal still works
			addChild(rowsTable);
			addChild(actual);
			if (before != null)
				addChild(before);
			if (after != null)
				addChild(after);
		}

		@Override
		public void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
			rowsTable.schedule(sc, es, scheduled);
			TriggerExecutionStep step = new TriggerExecutionStep(onTable.getPEDatabase(sc.getContext()),
					onTable.getStorageGroup(sc.getContext()),
					buildSubSequence(sc,actual,es.getPlan()),
					(before == null ? null : buildSubSequence(sc,before,es.getPlan())),
					(after == null ? null : buildSubSequence(sc,after,es.getPlan())),
					buildSubSequence(sc,rowQuery,es.getPlan()),
					handlers);
			es.append(step);
		}
		
	}

	protected static class AutoincrementTriggerValueHandler extends TriggerValueHandler {

		private final PEColumn column;
		
		public AutoincrementTriggerValueHandler(PEColumn col) {
			super(col.getType());
			this.column = col;
		}

		public Object onTarget(ExecutionState estate, Object beforeValue) {
			long extant = 0;
			if (beforeValue instanceof Number) {
				Number n = (Number) beforeValue;
				extant = n.longValue();
			}
			if (extant > 0) {
				AutoIncrementTracker.removeValue(estate.getCatalogDAO(),
						column.getTable().asTable().getAutoIncrTrackerID(),
						extant);
				return beforeValue;
			} else {
				return AutoIncrementTracker.getNextValue(estate.getCatalogDAO(), 
						column.getTable().asTable().getAutoIncrTrackerID());
			}
		}

		public boolean hasTarget() {
			return true;
		}

	}
}
