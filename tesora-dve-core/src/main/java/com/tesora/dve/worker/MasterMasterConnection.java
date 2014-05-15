// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PESQLException;

public class MasterMasterConnection extends SingleDirectConnection {

	public MasterMasterConnection(UserAuthentication auth, StorageSite site) {
		super(auth, site);
	}

	@Override
	protected void onCommunicationsFailure(Worker w) throws PESQLException {
		w.onCommunicationsFailure();
	}

	@Override
	protected SingleDirectStatement getNewStatement(Worker w) throws PESQLException {
		return new MasterMasterStatement(w, getConnection());
	}
}
