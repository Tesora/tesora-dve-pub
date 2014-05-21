// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.StatementType;
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
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
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
