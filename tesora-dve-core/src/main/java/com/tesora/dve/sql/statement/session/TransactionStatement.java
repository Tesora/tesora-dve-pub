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
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.CacheableStatement;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransactionExecutionStep;
import com.tesora.dve.sql.util.ListSet;

public class TransactionStatement extends SessionStatement implements CacheableStatement {

	// start, abort, commit
	public enum Kind {
		START,
		ROLLBACK,
		COMMIT,
		// XA only
		PREPARE,
		END
	}
	
	private final Kind kind;
	
	public TransactionStatement(Kind k) {
		super();
		this.kind = k;
	}
	
	public Kind getKind() {
		return this.kind;
	}
	
	public boolean isConsistent() {
		return false;
	}

	public UserXid getXAXid() {
		return null;
	}
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
		// I suppose we could make these things cacheable if we remembered the literals
		if (es.getPlan() != null && (getXAXid() == null))
			es.getPlan().setCacheable(true);
		es.append(new TransactionExecutionStep(getDatabase(sc),getStorageGroup(sc),this));
	}
	
	@Override
	public Database<?> getDatabase(SchemaContext pc) {
		return pc.getCurrentDatabase(false);
	}

	public static TransactionStatement buildStart() {
		return new TransactionStatement(Kind.START);
	}
	
	public static TransactionStatement buildCommit() throws PEException {
		return new TransactionStatement(Kind.COMMIT);
	}
	
	public static TransactionStatement buildRollback() throws PEException {
		return new RollbackTransactionStatement(null);
	}
	
	@Override
	public StatementType getStatementType() {
		return kind == Kind.COMMIT ? StatementType.COMMIT : StatementType.BEGIN;
	}

	private static final ListSet<TableKey> empty = new ListSet<TableKey>();
	
	@Override
	public ListSet<TableKey> getAllTableKeys() {
		return empty;
	}

}
