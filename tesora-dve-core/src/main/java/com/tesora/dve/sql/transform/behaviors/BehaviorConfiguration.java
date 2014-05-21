package com.tesora.dve.sql.transform.behaviors;

import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;

public interface BehaviorConfiguration extends FeaturePlannerBehavior {

	/*
	 * Find appropriate feature planners based on context & stmt.  You can override this to add feature planners arbitrarily.
	 */
	public FeaturePlanner[] getFeaturePlanners(PlannerContext pc, DMLStatement dmls);

	/*
	 * Find feature planners for select statements.
	 */
	public FeaturePlanner[] getSelectStatementPlanners(PlannerContext pc, DMLStatement dmls);

	/*
	 * Find feature planners for update statements.
	 */
	public FeaturePlanner[] getUpdateStatementPlanners(PlannerContext pc, DMLStatement dmls);

	/*
	 * Find feature planners for delete statements.
	 */
	public FeaturePlanner[] getDeleteStatementPlanners(PlannerContext pc, DMLStatement dmls);
	
	/*
	 * Find feature planners for insert into select statements
	 */
	public FeaturePlanner[] getInsertIntoSelectPlanners(PlannerContext pc, DMLStatement dmls);

	/*
	 * Find feature planners for union statements.
	 */
	public FeaturePlanner[] getUnionStatementPlanners(PlannerContext pc, DMLStatement dmls);
	
	/*
	 * Find feature planners for replace into select statements.
	 */
	public FeaturePlanner[] getReplaceIntoSelectPlanners(PlannerContext pc, DMLStatement dmls);

	/*
	 * Find feature planners for replace into values statements.
	 */
	public FeaturePlanner[] getReplaceIntoValuesPlanners(PlannerContext pc, DMLStatement dmls);

	/*
	 * Find feature planners for truncate statements.
	 */
	public FeaturePlanner[] getTruncatePlanners(PlannerContext pc, DMLStatement dmls);
	
	/*
	 * If you need to transform the plan after regular dml planning is complete, provide a plan transformer
	 */
	public FeaturePlanTransformerBehavior getPostPlanningTransformer(PlannerContext pc, DMLStatement original);
	
	// accessors for builtin planners - you can override individual planners as needed

}
