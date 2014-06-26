package com.tesora.dve.queryplan;

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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepDDLNestedOperation extends QueryStepDDLGeneralOperation {

	protected NestedOperationDDLCallback nestedOp;
	
	public QueryStepDDLNestedOperation(PersistentDatabase execCtxDBName, NestedOperationDDLCallback cb) {
		super(execCtxDBName);
		nestedOp = cb;
		setEntities(cb);
	}

	@Override
	protected void executeAction(SSConnection conn, CatalogDAO c, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		nestedOp.executeNested(conn, wg, resultConsumer);
	}

	@Override
	protected void prepareAction(SSConnection ssCon, CatalogDAO c, WorkerGroup wg, DBResultConsumer resultConsumer) throws PEException {
		nestedOp.prepareNested(ssCon, c, wg, resultConsumer);
	}

	
	@Override
	public boolean requiresWorkers() {
		return true;
	}

	
	public static abstract class NestedOperationDDLCallback extends DDLCallback {
		
		public abstract void executeNested(SSConnection conn, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable;

		public void prepareNested(SSConnection conn, CatalogDAO c, WorkerGroup wg, DBResultConsumer resultConsumer) throws PEException {
			
		}
		
		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return SQLCommand.EMPTY;
		}


	}

}
