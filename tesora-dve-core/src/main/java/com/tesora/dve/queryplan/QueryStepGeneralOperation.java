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

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepGeneralOperation extends QueryStepOperation {

	private AdhocOperation target;
	private boolean requiresWorkers;
	private boolean requiresTxn;
		
	public QueryStepGeneralOperation(AdhocOperation adhoc, boolean requiresTxn, boolean requiresWorkers) {
		super();
		target = adhoc;
		this.requiresWorkers = requiresWorkers;
		this.requiresTxn = requiresTxn;
	}
	
	public QueryStepGeneralOperation(AdhocOperation adhoc) {
		this(adhoc, false, false);
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		target.execute(ssCon, wg, resultConsumer);
	}

	@Override
	public boolean requiresTransaction() {
		return requiresTxn;
	}
	
	/*
	 * Does this query step require workers?  Default is true.  Some DDL operations are catalog-only, and
	 * thus do not require workers.
	 */
	@Override
	public boolean requiresWorkers() {
		return requiresWorkers;
	}

	
	public interface AdhocOperation {
		
		public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable;

	}

	
}
