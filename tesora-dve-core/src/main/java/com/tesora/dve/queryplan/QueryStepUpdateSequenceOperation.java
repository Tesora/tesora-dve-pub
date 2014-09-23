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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepUpdateSequenceOperation extends QueryStepOperation {

	private List<QueryStepOperation> ops = new ArrayList<QueryStepOperation>();
	
	public QueryStepUpdateSequenceOperation() {
		super();
	}
	
	public void addOperation(QueryStepOperation qso) {
		ops.add(qso);
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		for (QueryStepOperation qso : ops)
			qso.execute(ssCon, wg, resultConsumer);
	}

	@Override
	public boolean requiresTransaction() {
		return Functional.any(ops, new UnaryPredicate<QueryStepOperation>() {

			@Override
			public boolean test(QueryStepOperation object) {
				return object.requiresTransaction();
			}
			
		});
	}
	
	@Override
	public boolean requiresWorkers() {
		return Functional.any(ops, new UnaryPredicate<QueryStepOperation>() {

			@Override
			public boolean test(QueryStepOperation object) {
				return object.requiresWorkers();
			}
			
		});
	}
	
	@Override
	public boolean requiresImplicitCommit() {
		return Functional.any(ops, new UnaryPredicate<QueryStepOperation>() {

			@Override
			public boolean test(QueryStepOperation object) {
				return object.requiresImplicitCommit();
			}
			
		});
	}	

}
