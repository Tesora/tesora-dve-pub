// OS_STATUS: public
package com.tesora.dve.queryplan;

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
		resultConsumer.setSenderCount(ops.size());
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
