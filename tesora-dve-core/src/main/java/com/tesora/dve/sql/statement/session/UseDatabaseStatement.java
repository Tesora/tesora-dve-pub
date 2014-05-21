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

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.worker.WorkerGroup;

public class UseDatabaseStatement extends UseStatement {

	private final Database<?> target;
	
	public UseDatabaseStatement(Database<?> targ) {
		super(null);
		target = targ;
	}
	
	@Override
	public Database<?> getDatabase(SchemaContext pc) {
		return target;
	}	
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		es.append(new TransientSessionExecutionStep(getSQL(pc),new AdhocOperation() {
			@Override
			public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
				ssCon.setCurrentDatabase(target);
				ssCon.setCurrentTenant(null);
				// does not blow away the container - orthogonal
			}
		}));
	}

}
