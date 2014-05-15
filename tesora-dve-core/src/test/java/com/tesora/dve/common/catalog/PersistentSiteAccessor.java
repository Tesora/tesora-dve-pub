// OS_STATUS: public
package com.tesora.dve.common.catalog;

import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.worker.FailingMasterMasterWorker;
import com.tesora.dve.worker.FailingSingleWorker;

public class PersistentSiteAccessor {
	
	public static void injectTestClasses() {
		PersistentSite.workerFactoryMap.put(FailingSingleWorker.HA_TYPE, new FailingSingleWorker.Factory());
		PersistentSite.workerFactoryMap.put(FailingMasterMasterWorker.HA_TYPE, new FailingMasterMasterWorker.Factory());
	}

}
