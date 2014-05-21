// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

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



import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepBeginTransactionOperation;
import com.tesora.dve.queryplan.QueryStepCommitTransactionOperation;
import com.tesora.dve.queryplan.QueryStepEndXATransactionOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepPrepareXATransactionOperation;
import com.tesora.dve.queryplan.QueryStepRollbackTransactionOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.session.TransactionStatement;
import com.tesora.dve.sql.statement.session.XACommitTransactionStatement;
import com.tesora.dve.sql.statement.session.TransactionStatement.Kind;

public class TransactionExecutionStep extends ExecutionStep {

	private TransactionStatement stmt;
	
	public TransactionExecutionStep(Database<?> db, PEStorageGroup storageGroup, TransactionStatement ts) {
		super(db, storageGroup, ExecutionType.TRANSACTION);
		stmt = ts;
	}

	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		buf.add(stmt.getSQL(sc,opts, false));
	}

	public static ExecutionStep buildStart(SchemaContext pc, Database<?> peds) throws PEException {
		return new TransactionExecutionStep(peds, peds.getDefaultStorage(pc),
				TransactionStatement.buildStart());
	}

	public static ExecutionStep buildCommit(SchemaContext pc, Database<?> peds) throws PEException {
		return new TransactionExecutionStep(peds, peds.getDefaultStorage(pc),
				TransactionStatement.buildCommit());
	}

	public static ExecutionStep buildRollback(SchemaContext pc, Database<?> peds) throws PEException {
		return new TransactionExecutionStep(peds, peds.getDefaultStorage(pc),
				TransactionStatement.buildRollback());
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		QueryStepOperation qso = null;
		Kind txn = stmt.getKind();
		if (txn == Kind.START) {
			QueryStepBeginTransactionOperation qsbto = new QueryStepBeginTransactionOperation();
			if (stmt.isConsistent())
				qsbto.withConsistentSnapshot();
			qsbto.withXAXid(stmt.getXAXid());
			qso = qsbto;
		}
		else if (txn == Kind.COMMIT) {
			QueryStepCommitTransactionOperation qscto = new QueryStepCommitTransactionOperation();
			if (stmt.getXAXid() != null) {
				XACommitTransactionStatement xac = (XACommitTransactionStatement) stmt;
				qscto.withXAXid(xac.getXAXid(), xac.isOnePhase());
			}
			qso = qscto;
		}
		else if (txn == Kind.ROLLBACK) {
			QueryStepRollbackTransactionOperation qsrto = new QueryStepRollbackTransactionOperation();
			if (stmt.getXAXid() != null)
				qsrto.withXAXid(stmt.getXAXid());
			qso = qsrto;
		} else if (txn == Kind.PREPARE) {
			qso = new QueryStepPrepareXATransactionOperation(stmt.getXAXid());
		} else if (txn == Kind.END) {
			qso = new QueryStepEndXATransactionOperation(stmt.getXAXid());
		}
		addStep(sc, qsteps,qso);
	}
	
}
