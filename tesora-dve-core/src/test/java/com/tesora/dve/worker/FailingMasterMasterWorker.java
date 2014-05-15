// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.common.catalog.ISiteInstance;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.MasterMasterWorker;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerConnection;

public class FailingMasterMasterWorker extends MasterMasterWorker {

	public static class Factory extends MasterMasterWorker.Factory {
		@Override
		public Worker newWorker(UserAuthentication auth, StorageSite site)
				throws PEException {
			return new FailingMasterMasterWorker(auth, site);
		}
		@Override
		public String getInstanceIdentifier(StorageSite site, ISiteInstance instance) {
			return new StringBuffer(site.getName()).append("_").append(instance.getId()).toString();
		}
	}

	public static final String HA_TYPE = "FailingMasterMaster";

	public FailingMasterMasterWorker(UserAuthentication auth, StorageSite site)
			throws PEException {
		super(auth, site);
	}

	@Override
	public WorkerConnection getConnection(StorageSite site, UserAuthentication auth) {
		return new FailingMasterMasterConnection(auth, site);
	}

}
