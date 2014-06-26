package com.tesora.dve.worker;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.server.connectionmanager.log.ShutdownLog;
import com.tesora.dve.worker.agent.Agent;
import io.netty.channel.EventLoopGroup;
import org.apache.log4j.Logger;

import com.tesora.dve.common.RemoteException;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.server.messaging.WorkerManagerRequest;

public class WorkerManager extends Agent {
	
	Logger logger = Logger.getLogger(WorkerManager.class);
	
	Set<Worker> activeWorkers = new HashSet<Worker>();
	
	public WorkerManager() throws PEException {
		super(WorkerManager.class.getSimpleName());
	}

	@Override
	public void onMessage(Envelope e) throws PEException {
		Object payload = e.getPayload();	
		if (payload instanceof WorkerManagerRequest) {
			WorkerManagerRequest m = (WorkerManagerRequest)payload;
			ResponseMessage resp;
			try {
				resp =  m.executeRequest(e, this);
			} catch (Exception ex) {
				resp = new GenericResponse().setException(new RemoteException(getName(), ex));
			}
			if (resp != null)
				returnResponse(e, resp);
		} else {
			throw new PEException("WorkerManager received message of invalid type (" + payload.toString() + ")");
		}
	}

	public Worker getWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop) throws PEException {
		Worker theWorker = site.createWorker(auth, additionalConnInfo, preferredEventLoop);
		activeWorkers.add(theWorker);
		return theWorker;
	}
	
	private void returnWorker(Worker w) throws PEException {
		activeWorkers.remove(w);
		w.close();
	}
	
	public void returnWorkerList(StorageGroup group, Collection<Worker> theWorkers) throws PEException {
		ArrayList<StorageSite> groupSites = new ArrayList<StorageSite>();
		for (Worker worker : theWorkers) {
			groupSites.add(worker.getWorkerSite());
			returnWorker(worker);
		}
		group.returnWorkerSites(this, groupSites);
	}
	
	public boolean allWorkersReturned() throws PEException {
		boolean allWorkersReturned =  (activeWorkers.size() == 0);
		for (Worker w : activeWorkers)
			logger.error("active worker: " + w);
		return allWorkersReturned;
	}

	private void quiesceWorkerManager() throws PEException {
		sendAndReceive(newEnvelope(new WorkerManagerSync()).to(getAddress()));
	}
	
	public void shutdown() {
		try {
			quiesceWorkerManager();
			super.close();
			activeWorkers.clear();
		} catch (PEException e) {
			ShutdownLog.logShutdownError("Error shutting down " + getClass().getSimpleName(), e);
		}
	}

	public Map<StorageSite, Worker> getWorkerMap(UserAuthentication auth,
			AdditionalConnectionInfo additionalConnInfo, EventLoopGroup preferredEventLoop, Collection<? extends StorageSite> storageSites) throws PEException {
		Map<StorageSite, Worker> workerMap = new HashMap<StorageSite, Worker>();
		
		for (StorageSite site : storageSites) {
			workerMap.put(site, getWorker(auth, additionalConnInfo, site, preferredEventLoop));
		}
		return workerMap;
	}
}
