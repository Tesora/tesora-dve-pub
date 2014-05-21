// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransactionExecutionStep;

public class RollbackTransactionStatement extends TransactionStatement {

	private final Name savepoint;
	
	public RollbackTransactionStatement(Name savepointName) {
		super(Kind.ROLLBACK);
		savepoint = savepointName;
	}
	
	public Name getSavepointName() {
		return savepoint;
	}
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		if (savepoint == null)
			es.append(new TransactionExecutionStep(getDatabase(pc),getStorageGroup(pc),this));
		else
			unsupportedStatement();
	}
	
	@Override
	public StatementType getStatementType() {
		return StatementType.ROLLBACK;
	}
}
