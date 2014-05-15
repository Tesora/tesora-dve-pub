// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.worker.MasterMasterConnection;
import com.tesora.dve.worker.SingleDirectStatement;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

public class FailingMasterMasterConnection extends MasterMasterConnection {

	public FailingMasterMasterConnection(UserAuthentication auth, StorageSite site) {
		super(auth, site);
	}

	@Override
	protected SingleDirectStatement getNewStatement(Worker w) throws PESQLException {
		return new FailingMasterMasterStatement(w, getConnection());
	}
}
