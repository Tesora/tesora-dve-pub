package com.tesora.dve.server.connectionmanager.messages;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
import com.tesora.dve.server.connectionmanager.Transaction2PCTracker;
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
