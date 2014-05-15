// OS_STATUS: public
package com.tesora.dve.common.catalog;

import java.util.Map;

import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

public interface StorageSite {

	String getName();

	String getMasterUrl();

	String getInstanceIdentifier();

	/**
	 * getRecoverableSite() returns a site to be included in transaction recovery.  If the site
	 * does not participate in a transaction that needs to be recovered, this method should return
	 * null. (Dynamic sites should always return null)
	 * @return a PersistentSite, or null
	 */
	PersistentSite getRecoverableSite(CatalogDAO c);

	// just copy pickWorker from PersisentSite
	Worker pickWorker(Map<StorageSite, Worker> workerMap) throws PEException;

	void annotateStatistics(LogSiteStatisticRequest sNotice);

	void incrementUsageCount();

	Worker createWorker(UserAuthentication auth) throws PEException;

	/**
	 * Called when the notification manager decides that 
	 * a site can no longer be reached
	 * @param site
	 * @return TODO
	 * @throws PEException 
	 */
	void onSiteFailure(CatalogDAO c) throws PEException;

	int getMasterInstanceId();
	
	boolean supportsTransactions();
	
	boolean hasDatabase(UserVisibleDatabase ctxDB);
	
	void setHasDatabase(UserVisibleDatabase ctxDB) throws PEException;
}