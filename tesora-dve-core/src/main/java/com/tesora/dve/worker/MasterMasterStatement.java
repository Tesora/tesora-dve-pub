// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.db.DBConnection;
import com.tesora.dve.exceptions.PESQLException;


public class MasterMasterStatement extends SingleDirectStatement {

	public MasterMasterStatement(Worker w, DBConnection jdbcConnection)
			throws PESQLException {
		super(w, jdbcConnection);
	}

	@Override
	protected void onCommunicationFailure(Worker worker) throws PESQLException {
		worker.onCommunicationsFailure();
	}
}
