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
