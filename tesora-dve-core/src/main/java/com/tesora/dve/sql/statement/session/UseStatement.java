// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class UseStatement extends SessionStatement {

	private final Persistable<?,?> target;
	
	public UseStatement(Persistable<?,?> targ) {
		super();
		target = targ;
	}
	
	public Persistable<?,?> getTarget() {
		return target;
	}	
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		throw new PEException("Unsupported use kind");
	}
}
