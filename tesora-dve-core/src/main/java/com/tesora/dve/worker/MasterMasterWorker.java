// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.common.catalog.ISiteInstance;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;

public class MasterMasterWorker extends Worker {

	public static class Factory implements Worker.Factory {
		@Override
		public Worker newWorker(UserAuthentication auth, StorageSite site)
				throws PEException {
			return new MasterMasterWorker(auth, site);
		}

		@Override
		public void onSiteFailure(StorageSite site) throws PEException {
			// We know a MasterMasterWorker can only work on a PersistentSite, so to 
			// avoid implementing empty methods everywhere, we'll cast
			((PersistentSite)site).ejectMaster();
		}

		@Override
		public String getInstanceIdentifier(StorageSite site, ISiteInstance instance) {
			return site.getName();
		}
	}

	public static final String HA_TYPE = "MasterMaster";

	public MasterMasterWorker(UserAuthentication auth, StorageSite site)
			throws PEException {
		super(auth, site);
	}

	@Override
	public WorkerConnection getConnection(StorageSite site, UserAuthentication auth) {
		return new MasterMasterConnection(auth, site);
	}
}
