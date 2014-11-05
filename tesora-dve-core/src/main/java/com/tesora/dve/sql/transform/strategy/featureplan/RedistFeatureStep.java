package com.tesora.dve.sql.transform.strategy.featureplan;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKeyTemplate;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.RedistributionExecutionStep;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class RedistFeatureStep extends FeatureStep {


	private final ProjectingFeatureStep source;
	private final PEStorageGroup targetGroup;
	private final TableKey targetTable;
	private final List<Integer> keyOffsets;
	private final RedistributionFlags flags;

	public RedistFeatureStep(FeaturePlanner planner, ProjectingFeatureStep src,TableKey targetTable, PEStorageGroup targetGroup,
			List<Integer> distOnOffsets,
			RedistributionFlags flags) {
		super(planner, src.getSourceGroup(), src.getDistributionKey());
		this.source = src;
		this.targetTable = targetTable;
		this.targetGroup = targetGroup;
		this.keyOffsets = distOnOffsets;
		this.flags = flags;
		// we assume our src's children
		getSelfChildren().addAll(src.getAllChildren());
	}

	public ProjectingFeatureStep getSourceStep() {
		return source;
	}
	
	private static final RedistributionFlags missingFlags = new RedistributionFlags();
	
	@Override
	public void scheduleSelf(PlannerContext pc, ExecutionSequence es)
			throws PEException {		
		DistributionKeyTemplate dkt = new DistributionKeyTemplate(targetTable.getAbstractTable());
		int counter = 1;
		List<ExpressionNode> projection = ((ProjectingStatement)source.getPlannedStatement()).getProjections().get(0);
		for(Integer i : keyOffsets) {
			dkt.addColumn(projection.get(i.intValue()), counter++);
		}
		RedistributionFlags rf = (flags == null ? missingFlags : flags);
		es.append(RedistributionExecutionStep.build(pc.getContext(),
				source.getDatabase(pc),
				source.getSourceGroup(),
				source.getDistributionVector(),
				(ProjectingStatement)source.getPlannedStatement(),
				targetGroup,
				targetTable.getAbstractTable().asTable(),
				(targetTable instanceof MTTableKey ? ((MTTableKey)targetTable).getScope() : null),
				dkt,
				rf.getMissingAutoIncrement(),
				rf.getOffsetOfExistingAutoInc(),
				rf.getOnDuplicateKey(),
				rf.getUseRowCount(),
				source.getDistributionKey(),
				rf.isMustEnforceScalarValue(),
				rf.isInsertIgnore(),
				rf.getGenerator(),
				getExplainRecord()));
		
	}

	@Override
	public DMLStatement getPlannedStatement() {
		return source.getPlannedStatement();
	}

	@Override
	public Database<?> getDatabase(PlannerContext pc) {
		return source.getDatabase(pc);
	}
	
	public TableKey getTargetTable() {
		return targetTable;
	}
	
	public ProjectingFeatureStep buildNewProjectingStep(PlannerContext pc, FeaturePlanner planner, ExecutionCost newCost, DMLExplainRecord splain) throws PEException {
		TempTable tt = getTargetTempTable();
		if (tt == null) return null;
		SelectStatement intent = tt.buildSelect(pc.getContext());
		ProjectingFeatureStep out = 
				DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(pc, planner, 
						intent,
						(newCost != null ? newCost : source.getCost()),
						targetGroup,
						getPlannedStatement().getDatabase(pc.getContext()),
						tt.getDistributionVector(pc.getContext()),
						null, 
						splain);
		out.addChild(this);
		return out;
	}
	
	public ProjectingFeatureStep buildTriggerRowsStep(PlannerContext pc, FeaturePlanner planner) throws PEException {
		TempTable tt = getTargetTempTable();
		if (tt == null) return null;
		SelectStatement intent = tt.buildSelect(pc.getContext());
		ProjectingFeatureStep out = 
				DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(pc, planner, 
						intent,
						source.getCost(),
						targetGroup,
						getPlannedStatement().getDatabase(pc.getContext()),
						tt.getDistributionVector(pc.getContext()),
						null, 
						null);
		// don't thread self in - the step is executed outside of the normal flow
		return out;
	}
	
	
	public SelectStatement buildNewSelect(PlannerContext pc) throws PEException {
		TempTable tt = getTargetTempTable();
		if (tt == null) return null;
		SelectStatement intent = tt.buildSelect(pc.getContext());
		return intent;
	}
	
	public TempTable getTargetTempTable() {
		if (targetTable.getAbstractTable().isTempTable()) {
			return (TempTable) targetTable.getAbstractTable();
		}
		return null;
			
	}
	
}
