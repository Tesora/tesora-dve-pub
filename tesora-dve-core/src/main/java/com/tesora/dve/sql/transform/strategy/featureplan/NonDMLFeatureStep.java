// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public abstract class NonDMLFeatureStep extends FeatureStep {

	public NonDMLFeatureStep(FeaturePlanner planner, PEStorageGroup srcGroup) {
		super(planner, srcGroup, null);
		withDefangInvariants();
	}

	@Override
	public DMLStatement getPlannedStatement() {
		return null;
	}

	@Override
	public boolean isDML() {
		return false;
	}
	
	@Override
	public final Database<?> getDatabase(PlannerContext pc) {
		return null;
	}
	
}
