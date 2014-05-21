// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.util.ListOfPairs;

public class LockStatement extends SessionStatement {

	// if empty, this is an unlock
	private ListOfPairs<TableInstance, LockType> locks;
	
	public LockStatement() {
		this(null);
	}
	
	public LockStatement(ListOfPairs<TableInstance, LockType> locks) {
		super();
		this.locks = locks;
	}
	
	public boolean isUnlock() {
		return this.locks == null || this.locks.isEmpty();
	}
	
	public ListOfPairs<TableInstance, LockType> getLocks() {
		return locks;
	}	
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		// Both LOCK and UNLOCK will do implicit commits
		// and likely for safety we should wrap them in something that freezes the worker set.  A user txn
		// does not work, because the explicit begin transaction followed by a LOCK results in one of the XAER_*
		// exceptions.
		/*
		if (isUnlock()) {
			es.append(TransactionExecutionStep.buildCommit(getDatabase()));
			super.plan(es);
		} else {
			super.plan(es);
			es.append(TransactionExecutionStep.buildStart(getDatabase()));
		}
		*/
		if (isUnlock())
			sc.getIntraStmtState().setSawUnlockTable();
		else
			sc.getIntraStmtState().setSawLockTable();
		es.append(new EmptyExecutionStep(0,"unsupported execution: " + getSQL(sc)));
	}
	
	@Override
	public StatementType getStatementType() {
		return this.isUnlock() ? StatementType.UNLOCK_TABLES : StatementType.LOCK_TABLES;
	}
}
