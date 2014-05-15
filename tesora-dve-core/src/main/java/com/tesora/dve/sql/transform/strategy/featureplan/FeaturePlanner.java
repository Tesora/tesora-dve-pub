// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public interface FeaturePlanner {

	// return the step if we apply, elsewise null
	// note that parent information is stored in the planner context
	public FeatureStep plan(DMLStatement stmt, PlannerContext context) throws PEException;
	
	public FeaturePlannerIdentifier getFeaturePlannerID();

	public boolean emitting();
	
	public void emit(String what);
}
