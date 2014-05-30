package com.tesora.dve.sql.transform.behaviors;

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
