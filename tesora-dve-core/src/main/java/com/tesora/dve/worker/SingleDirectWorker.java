// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.common.catalog.ISiteInstance;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;

public class SingleDirectWorker extends Worker {
	
//	private Logger logger = Logger.getLogger(SingleJDBCWorker.class);

	public static class Factory implements Worker.Factory {
		@Override
		public Worker newWorker(UserAuthentication auth, StorageSite site)
				throws PEException {
			return new SingleDirectWorker(auth, site);
		}

		@Override
		public void onSiteFailure(StorageSite site) {
		}

		@Override
		public String getInstanceIdentifier(StorageSite site,
				ISiteInstance instance) {
			return site.getName();
		}
	}

	public static final String HA_TYPE = "Single";

	protected SingleDirectWorker(UserAuthentication auth, StorageSite site) throws PEException {
		super(auth, site);
	}

	@Override
	public WorkerConnection getConnection(StorageSite site, UserAuthentication auth) {
		SingleDirectConnection wConnection = new SingleDirectConnection(auth, site);
//		if (logger.isDebugEnabled())
//			try {
//				logger.debug("Worker " + getName() + " gets JDBC connection " + wConnection.getConnection().toString());
//			} catch (PESQLException e) {} 
		return wConnection;
	}
}
