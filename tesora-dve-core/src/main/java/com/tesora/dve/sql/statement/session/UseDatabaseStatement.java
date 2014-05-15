// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

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
