// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.SingleDirectWorker;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerConnection;

public class FailingSingleWorker extends SingleDirectWorker {
	
	public static class Factory extends SingleDirectWorker.Factory {

		@Override
		public Worker newWorker(UserAuthentication auth, StorageSite site)
				throws PEException {
			return new FailingSingleWorker(auth, site);
		}
		
	}

	public static final String HA_TYPE = "FailingSingle";

	public FailingSingleWorker(UserAuthentication auth, StorageSite site)
			throws PEException {
		super(auth, site);
	}

	@Override
	public WorkerConnection getConnection(StorageSite site, UserAuthentication auth) {
		return new FailingMasterMasterConnection(auth, site);
	}

}
