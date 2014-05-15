// OS_STATUS: public
package com.tesora.dve.sql.transform.behaviors;

import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;

public interface FeatureStepBuilder extends FeaturePlannerBehavior {

	// build a step in general - non projecting
	public FeatureStep 
		buildStep(PlannerContext pc, FeaturePlanner planner, DMLStatement ds, DistributionKey distKey, DMLExplainRecord explain);
	
	// build a projecting step - separate method due to costing
	public ProjectingFeatureStep 
		buildProjectingStep(
				PlannerContext pc,
				FeaturePlanner planner,
				ProjectingStatement statement, 
				ExecutionCost cost, 
				PEStorageGroup group, 
				Database<?> db, 
				DistributionVector vector,
				DistributionKey distKey,
				DMLExplainRecord explain);
	
}
