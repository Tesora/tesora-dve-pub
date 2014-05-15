// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
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
	public boolean requiresWorkers() {
		return true;
	}

	
	public interface NestedOperationDDLCallback extends DDLCallback {
		
		public void executeNested(SSConnection conn, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable;
		
	}
	
}
