// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.WorkerRecoverSiteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.server.transactionmanager.Transaction2PCTracker;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class GlobalRecoveryExecutor implements AgentExecutor<SSConnection> {

	@Override
	public ResponseMessage execute(SSConnection ssCon, Object message) throws Throwable {
		
		Collection<PersistentSite> uniqueSites = getUniqueSites(ssCon);
		
		PersistentGroup uberGroup = new PersistentGroup(uniqueSites);
		Map<String, Boolean> commitMap = Transaction2PCTracker.getGlobalCommitMap();

		WorkerGroup wg = WorkerGroupFactory.newInstance(ssCon, uberGroup, null);
		try {
			WorkerRequest req = new WorkerRecoverSiteRequest(ssCon.getNonTransactionalContext(), commitMap);
			wg.execute(MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE);
//			wg.sendToAllWorkers(ssCon, req);
//			ssCon.consumeReplies(wg.size());
		} finally {
			WorkerGroupFactory.purgeInstance(ssCon, wg);
		}
		
		Transaction2PCTracker.clearOldTransactionRecords();
		
		return new GenericResponse().success();
	}

	private Collection<PersistentSite> getUniqueSites(SSConnection ssCon) {
		List<PersistentSite> allSites = ssCon.getCatalogDAO().findAllPersistentSites();
		Map<String, PersistentSite> uniqueURLMap = new HashMap<String, PersistentSite>();
		for(PersistentSite site : allSites)
			uniqueURLMap.put(site.getMasterUrl(), site);
		Collection<PersistentSite> uniqueSites = uniqueURLMap.values();
		return uniqueSites;
	}

}
