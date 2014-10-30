package com.tesora.dve.sql.transform.behaviors.defaults;

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


import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.behaviors.FeaturePlanTransformerBehavior;
import com.tesora.dve.sql.transform.strategy.ContainerBaseTableRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.DegenerateExecuteTransformFactory;
import com.tesora.dve.sql.transform.strategy.DeleteRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.DistributionKeyExecuteTransformFactory;
import com.tesora.dve.sql.transform.strategy.GroupByRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.HavingRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.InformationSchemaRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.NestedQueryBroadcastTransformFactory;
import com.tesora.dve.sql.transform.strategy.NullLiteralColumnTransformFactory;
import com.tesora.dve.sql.transform.strategy.OrderByLimitRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.SessionRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.SingleSiteStorageGroupTransformFactory;
import com.tesora.dve.sql.transform.strategy.TruncateStatementMTTransformFactory;
import com.tesora.dve.sql.transform.strategy.UnionRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.UpdateRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.ViewRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.aggregation.GenericAggRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.correlated.ProjectionCorrelatedSubqueryTransformFactory;
import com.tesora.dve.sql.transform.strategy.correlated.WhereClauseCorrelatedSubqueryTransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.insert.InsertIntoSelectPlanner;
import com.tesora.dve.sql.transform.strategy.insert.InsertIntoValuesPlanner;
import com.tesora.dve.sql.transform.strategy.insert.ReplaceIntoTransformFactory;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.joinsimplification.JoinSimplificationTransformFactory;
import com.tesora.dve.sql.transform.strategy.nested.NestedQueryRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.triggers.DeleteTriggerPlanner;
import com.tesora.dve.sql.transform.strategy.triggers.InsertIntoTriggerPlanner;
import com.tesora.dve.sql.transform.strategy.triggers.ReplaceIntoTriggerPlanner;
import com.tesora.dve.sql.transform.strategy.triggers.UpdateTriggerPlanner;

/*
 * Default behavior configuration.  If you need to make modifications, subclass DelegatingBehaviorConfiguration and
 * specify this class as the target, then modify as needed.
 */
public final class DefaultBehaviorConfiguration implements BehaviorConfiguration {

	// TODO:
	// make single copies of all of the planners, and the arrays
	
	public FeaturePlanner[] getFeaturePlanners(PlannerContext pc, DMLStatement dmls) {
		if (dmls instanceof SelectStatement)
			return getSelectStatementPlanners(pc, dmls);
		else if (dmls instanceof UpdateStatement)
			return getUpdateStatementPlanners(pc, dmls);
		else if (dmls instanceof DeleteStatement)
			return getDeleteStatementPlanners(pc, dmls);
		else if (dmls instanceof UnionStatement)
			return getUnionStatementPlanners(pc, dmls);
		else if (dmls instanceof ReplaceIntoSelectStatement) 
			return getReplaceIntoSelectPlanners(pc,dmls);
		else if (dmls instanceof ReplaceIntoValuesStatement)
			return getReplaceIntoValuesPlanners(pc,dmls);
		else if (dmls instanceof InsertIntoValuesStatement)
			return getInsertIntoValuesPlanners(pc,dmls);
		else if (dmls instanceof InsertIntoSelectStatement)
			return getInsertIntoSelectPlanners(pc,dmls);
		else if (dmls instanceof TruncateStatement)
			return getTruncatePlanners(pc,dmls);
		else
			throw new SchemaException(Pass.PLANNER, "No feature planners configured for statement of type " + dmls.getClass().getName());
	}
	
	public FeaturePlanner[] getSelectStatementPlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new InformationSchemaRewriteTransformFactory(),
				new SessionRewriteTransformFactory(),
				new ViewRewriteTransformFactory(),
				new SingleSiteStorageGroupTransformFactory(),
				new NullLiteralColumnTransformFactory(),
				new OrderByLimitRewriteTransformFactory(),
				new ProjectionCorrelatedSubqueryTransformFactory(),
				new NestedQueryRewriteTransformFactory(),
				new HavingRewriteTransformFactory(),
				new GenericAggRewriteTransformFactory(),
				new GroupByRewriteTransformFactory(),
				new WhereClauseCorrelatedSubqueryTransformFactory(),
				new JoinSimplificationTransformFactory(),
				new JoinRewriteTransformFactory(),
				new DistributionKeyExecuteTransformFactory(),
				new DegenerateExecuteTransformFactory()
		};				
	}
	
	public FeaturePlanner[] getUpdateStatementPlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new InformationSchemaRewriteTransformFactory(),
				new UpdateTriggerPlanner(),
				new SessionRewriteTransformFactory(),
				new ViewRewriteTransformFactory(),
				new ContainerBaseTableRewriteTransformFactory(),
				new SingleSiteStorageGroupTransformFactory(),
				new NestedQueryBroadcastTransformFactory(),
				new NestedQueryRewriteTransformFactory(),
				new UpdateRewriteTransformFactory(),
				new DistributionKeyExecuteTransformFactory(),
				new DegenerateExecuteTransformFactory()
		};

	}
	
	public FeaturePlanner[] getDeleteStatementPlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new InformationSchemaRewriteTransformFactory(),
				new DeleteTriggerPlanner(),
				new SessionRewriteTransformFactory(),
				new ViewRewriteTransformFactory(),
				new ContainerBaseTableRewriteTransformFactory(),
				new SingleSiteStorageGroupTransformFactory(),
				new NestedQueryBroadcastTransformFactory(),
				new NestedQueryRewriteTransformFactory(),
				new DeleteRewriteTransformFactory(),
				new DistributionKeyExecuteTransformFactory(),
				new DegenerateExecuteTransformFactory()
		};

	}
	
	public FeaturePlanner[] getInsertIntoSelectPlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new InformationSchemaRewriteTransformFactory(),
				new InsertIntoTriggerPlanner(),
				new SessionRewriteTransformFactory(),
				new ViewRewriteTransformFactory(),
				new InsertIntoSelectPlanner(),
				new SingleSiteStorageGroupTransformFactory(),
		};

	}
	
	public FeaturePlanner[] getUnionStatementPlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new InformationSchemaRewriteTransformFactory(),
				new ViewRewriteTransformFactory(),
				new SingleSiteStorageGroupTransformFactory(),
				new UnionRewriteTransformFactory()	
			};
	}
	
	public FeaturePlanner[] getReplaceIntoSelectPlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new InformationSchemaRewriteTransformFactory(),
				new ReplaceIntoTriggerPlanner(),
				new SessionRewriteTransformFactory(),
				new ViewRewriteTransformFactory(),
				new ReplaceIntoTransformFactory()
			};
	}
	
	public FeaturePlanner[] getReplaceIntoValuesPlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new InformationSchemaRewriteTransformFactory(),
				new ReplaceIntoTriggerPlanner(),
				new SessionRewriteTransformFactory(),
				new ReplaceIntoTransformFactory(),
				// for simple cases we can use the insert into values planner
				new InsertIntoValuesPlanner()
			};
	}
	
	public FeaturePlanner[] getInsertIntoValuesPlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new InsertIntoValuesPlanner()
		};
	}
	
	public FeaturePlanner[] getTruncatePlanners(PlannerContext pc, DMLStatement dmls) {
		return new FeaturePlanner[] {
				new TruncateStatementMTTransformFactory(),
				new DegenerateExecuteTransformFactory()
		};

	}

	@Override
	public FeaturePlanTransformerBehavior getPostPlanningTransformer(
			PlannerContext pc, DMLStatement original) {
		return new DefaultPostPlanningFeaturePlanTransformer();
	}
}
