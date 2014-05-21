package com.tesora.dve.sql.transform.behaviors;

import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultBehaviorConfiguration;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;

/*
 * A delegating behavior configuration pass all method calls to the given behavior object.
 * You can override specific behaviors by overriding methods in this class.
 */
public class DelegatingBehaviorConfiguration implements BehaviorConfiguration {

	private final BehaviorConfiguration target;
	
	public DelegatingBehaviorConfiguration(BehaviorConfiguration target) {
		if (target == null)
			this.target = new DefaultBehaviorConfiguration();
		else
			this.target = target;
	}
	
	public BehaviorConfiguration getTarget() {
		return this.target;
	}
	
	@Override
	public FeaturePlanner[] getFeaturePlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getFeaturePlanners(pc, dmls);
	}

	@Override
	public FeaturePlanner[] getSelectStatementPlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getSelectStatementPlanners(pc, dmls);
	}

	@Override
	public FeaturePlanner[] getUpdateStatementPlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getUpdateStatementPlanners(pc, dmls);
	}

	@Override
	public FeaturePlanner[] getDeleteStatementPlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getDeleteStatementPlanners(pc, dmls);
	}

	@Override
	public FeaturePlanner[] getInsertIntoSelectPlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getInsertIntoSelectPlanners(pc, dmls);
	}

	@Override
	public FeaturePlanner[] getUnionStatementPlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getUnionStatementPlanners(pc, dmls);
	}

	@Override
	public FeaturePlanner[] getReplaceIntoSelectPlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getReplaceIntoSelectPlanners(pc, dmls);
	}

	@Override
	public FeaturePlanner[] getReplaceIntoValuesPlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getReplaceIntoValuesPlanners(pc, dmls);
	}

	@Override
	public FeaturePlanner[] getTruncatePlanners(PlannerContext pc,
			DMLStatement dmls) {
		return target.getTruncatePlanners(pc, dmls);
	}

	@Override
	public FeaturePlanTransformerBehavior getPostPlanningTransformer(
			PlannerContext pc, DMLStatement original) {
		return target.getPostPlanningTransformer(pc, original);
	}

}
