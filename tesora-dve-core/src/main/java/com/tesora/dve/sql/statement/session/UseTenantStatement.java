// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.worker.WorkerGroup;

public class UseTenantStatement extends UseStatement {

	PEDatabase db;
	
	public UseTenantStatement(PETenant pet, PEDatabase ped) {
		super(pet);
		db = ped;
	}

	public PETenant getTenant() {
		return (PETenant) getTarget();
	}
	
	@Override
	public PEDatabase getDatabase(SchemaContext pc) {
		return db;
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		pc.beginSaveContext();
		try {
			final PETenant ten = getTenant();
			es.append(new TransientSessionExecutionStep(getSQL(pc), new AdhocOperation() {
				@Override
				public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
					ssCon.setCurrentDatabase(db);
					ssCon.setCurrentTenant(ten);
				}
			}));
		} finally {
			pc.endSaveContext();
		}
	}
}
