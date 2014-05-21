// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class SavepointStatement extends SessionStatement {

	private final Name savepointName;
	private final boolean release;
	
	public SavepointStatement(Name n, boolean release) {
		super();
		savepointName = n;
		this.release = release;
	}
	
	public Name getSavepointName() {
		return savepointName;
	}
	
	public boolean isRelease() {
		return release;
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		es.append(new EmptyExecutionStep(0,"unsupported execution: " + getSQL(pc)));
	}

	
}
