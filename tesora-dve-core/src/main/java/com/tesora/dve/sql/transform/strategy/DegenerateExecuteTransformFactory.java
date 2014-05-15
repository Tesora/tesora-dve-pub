// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.behaviors.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public class DegenerateExecuteTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.DEGENERATE;
	}

	
	
	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context) throws PEException {
		List<PEStorageGroup> groups = stmt.getStorageGroups(context.getContext());
		if (groups.size() != 1) {
			throw new PEException("Multiple groups found in statement, further planning needed");
		}
		if (stmt instanceof SelectStatement) {
			return buildSelectPlan(context, (SelectStatement)stmt);
		} else if (stmt instanceof DeleteStatement) {
			return buildDeletePlan(context, (DeleteStatement)stmt);
		} else if (stmt instanceof UpdateStatement) {
			return buildUpdatePlan(context,(UpdateStatement)stmt);
		} else if (stmt instanceof InsertIntoValuesStatement) {
			// insert into values has it's own planner
			throw new PEException("Internal error: insert statements planned incorrectly");
		} else if (stmt instanceof TruncateStatement) {
			return buildTruncatePlan(context,(TruncateStatement)stmt);
		} else {
			throw new PEException("What kind of dml statement is " + stmt.getClass().getName());
		}

	}

	private FeatureStep buildTruncatePlan(PlannerContext pc, final TruncateStatement ts) throws PEException {
		return new FeatureStep(this,
				ts.getTruncatedTable().getAbstractTable().getStorageGroup(pc.getContext()),
				null) {

					@Override
					public void scheduleSelf(PlannerContext pc, ExecutionSequence es)
							throws PEException {
						final ExecutionStep aitReset = ts.buildResetAutoIncrementTrackerStep(pc.getContext());
						es.append(aitReset);
					}

					@Override
					public DMLStatement getPlannedStatement() {
						return ts;
					}

					@Override
					public Database<?> getDatabase(PlannerContext pc) {
						return ts.getTruncatedTable().getAbstractTable().getDatabase(pc.getContext());
					}
			
		}.withPlanner(this);
	}

	private FeatureStep buildUpdatePlan(PlannerContext pc, final UpdateStatement us) throws PEException {
		return DefaultFeatureStepBuilder.INSTANCE.buildStep(pc, this, us, null, null);
	}

	private FeatureStep buildDeletePlan(PlannerContext pc, final DeleteStatement ds) throws PEException {
		return DefaultFeatureStepBuilder.INSTANCE.buildStep(pc, this, ds, null, null);
	}

	private FeatureStep buildSelectPlan(PlannerContext pc, final SelectStatement ss) throws PEException {
		// see if we can predict how many sites are needed.  we didn't match the 
		// dist key transform, so we don't have enough info to figure that out - but we
		// can use the least inclusive dv to determine whether this is a single site query or not
		PEStorageGroup group = ss.getSingleGroup(pc.getContext());
		DistributionVector dv = EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(ss,pc.getContext());
		boolean bcast = (dv != null && dv.isBroadcast());
		ExecutionCost ec = null;
		if (pc.isCosting()) {
			ec = Costing.buildBasicCost(pc.getContext(), ss, bcast);
		} else {
			ec = new ExecutionCost(EngineConstant.WHERECLAUSE.has(ss),bcast,null,-1);
		}
		DMLExplainRecord rec = (ec.getRowCount() > -1 ?
										new DMLExplainRecord(DMLExplainReason.BASIC_PARTITION_QUERY,null,ec.getRowCount())
									: null);
		return DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(
				pc, 
				this,
				ss, 
				ec, 
				group, 
				(PEDatabase)ss.getDatabase(pc.getContext()),
				dv,
				null,
				rec);
	}
	
}
