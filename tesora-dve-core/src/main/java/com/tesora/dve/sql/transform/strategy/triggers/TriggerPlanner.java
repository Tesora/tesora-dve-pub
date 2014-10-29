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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LateBindingConstantExpression;
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
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
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
					new LateBindingConstantExpression(me.getValue())));
		}
		return ExpressionUtils.safeBuildAnd(eqs);
	}
	
	protected TriggerFeatureStep commonPlanning(PlannerContext context, TableKey targetTable, SelectStatement srcSelect, DMLStatement uniqueStatement,
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
		
		FeatureStep targetStep =  
				buildPlan(uniqueStatement,context.withTransform(getFeaturePlannerID()),
						DefaultFeaturePlannerFilter.INSTANCE);
		
		return new TriggerFeatureStep(this,targetTable.getAbstractTable().asTable(),
				rowsTable,rows,targetStep,
				triggerInfo.getBeforeStep(context.getContext()),
				triggerInfo.getAfterStep(context.getContext()));
	}
	
	protected static class TriggerFeatureStep extends MultiFeatureStep {

		private final RedistFeatureStep rowsTable;
		private final FeatureStep actual;
		private final FeatureStep before;
		private final FeatureStep after;
		private final FeatureStep rowQuery;
		private final PETable onTable;
		
		public TriggerFeatureStep(FeaturePlanner planner, PETable actualTable, RedistFeatureStep rowsTable, FeatureStep rowsQuery,
				FeatureStep actual, FeatureStep before, FeatureStep after) {
			super(planner);
			this.rowsTable = rowsTable;
			this.actual = actual;
			this.before = before;
			this.after = after;
			this.rowQuery = rowsQuery;
			this.onTable = actualTable;
			// make sure the traversal still works
			addChild(rowsTable);
			addChild(actual);
			if (before != null)
				addChild(before);
			if (after != null)
				addChild(after);
			withDefangInvariants();
		}

		@Override
		public void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
			rowsTable.schedule(sc, es, scheduled);
			TriggerExecutionStep step = new TriggerExecutionStep(onTable.getPEDatabase(sc.getContext()),
					onTable.getStorageGroup(sc.getContext()),
					buildSubSequence(sc,actual,es.getPlan()),
					(before == null ? null : buildSubSequence(sc,before,es.getPlan())),
					(after == null ? null : buildSubSequence(sc,after,es.getPlan())),
					buildSubSequence(sc,rowQuery,es.getPlan()));
			es.append(step);
		}
		
		private ExecutionStep buildSubSequence(PlannerContext pc, FeatureStep step, ExecutionPlan parentPlan) throws PEException {
			ExecutionSequence sub = new ExecutionSequence(parentPlan);
			step.schedule(pc,sub,new HashSet<FeatureStep>());
			if (sub.getSteps().size() == 1)
				return (ExecutionStep)sub.getSteps().get(0);
			return sub;
		}
	}

}
