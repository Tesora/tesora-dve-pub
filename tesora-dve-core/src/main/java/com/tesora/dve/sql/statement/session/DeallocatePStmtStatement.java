// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.PlanCacheUtils;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class DeallocatePStmtStatement extends PStmtStatement {

	public DeallocatePStmtStatement(UnqualifiedName unq) {
		super(unq);
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		PlanCacheUtils.destroyPreparedStatement(sc, getName().get());
		es.append(new EmptyExecutionStep(0,"deallocate " + getName()));
	}

	
}
