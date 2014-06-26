package com.tesora.dve.server.messaging;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerManager;
import com.tesora.dve.worker.agent.Envelope;
import io.netty.channel.EventLoopGroup;

public class GetWorkerRequest extends WorkerManagerRequest {
	
	private static final long serialVersionUID = 3107349676917943589L;
    EventLoopGroup preferredEventLoop;
	UserAuthentication userAuth;
	StorageGroup storageGroup;
	
	WorkerManager wm;
	Envelope requestEnvelope;
	
	 String siteClassName;
	 int siteCount;
	 boolean strict;
	
	public GetWorkerRequest(UserAuthentication userAuth, StorageGroup group, EventLoopGroup preferredEventLoop) {
		this.userAuth = userAuth;
		this.storageGroup = group;
        this.preferredEventLoop = preferredEventLoop;
//		group.prepareForTransport(); // to ensure that JPA loads the sites & gens
	}

	@Override
	public WorkerManagerResponse executeRequest(Envelope e, WorkerManager wm) throws PEException {
		this.wm = wm;
		this.requestEnvelope = e;
		
		if(storageGroup == null)
			throw new PEException("Can't execute GetWorkerRequest - storageGroup not defined");
		
		storageGroup.provisionGetWorkerRequest(this);
		return null;
	}
	
	public void fulfillGetWorkerRequest(Collection<? extends StorageSite> storageSites) throws PEException {
		Map<StorageSite, Worker> theWorkers = wm.getWorkerMap(userAuth, preferredEventLoop, storageSites);
		ResponseMessage resp = new GetWorkerResponse(theWorkers);
		wm.returnResponse(requestEnvelope, resp);
	}

	public UserAuthentication getUserAuth() {
		return userAuth;
	}

	public WorkerManager getWorkerManager() {
		return wm;
	}

	public StorageGroup getStorageGroup() {
		return storageGroup;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.WM_GET_WORKER_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

	@Override
	public String toString() {
		return PEStringUtils.toString(this.getClass(), Arrays.asList(new Object[]  {userAuth.getUserid(), storageGroup.getName()}));
	}

	public void addProvisioningDetails(String className, int count, boolean strict) {
		this.siteClassName = className;
		this.siteCount = count;
		this.strict = strict;
	}

	public String getSiteClassName() {
		return siteClassName;
	}

	public int getSiteCount() {
		return siteCount;
	}
	
	public boolean getIsStrict() {
		return strict;
	}
}
