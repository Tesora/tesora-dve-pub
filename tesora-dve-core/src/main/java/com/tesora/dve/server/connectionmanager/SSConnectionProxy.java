package com.tesora.dve.server.connectionmanager;

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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.worker.agent.Agent;

public class SSConnectionProxy extends Agent {
	
	// this is used so that we can have tests set context to help with debugging
	static String operatingContext = "RUNTIME";
	static Set<SSConnectionProxy> proxySet = Collections.newSetFromMap(new ConcurrentHashMap<SSConnectionProxy, Boolean>()); 
	

	String operatingContextInstance;
	SSConnection ssConnection;
	private Thread shutdownHook;

	public SSConnectionProxy() throws PEException {
		super("SSConnectionProxy");
	}
	
	@Override
	protected void initialize() throws PEException {
		ssConnection = new SSConnection();
		
		operatingContextInstance = operatingContext;
		if (!"RUNTIME".equals(operatingContextInstance)) {
			proxySet.add(this);
		}
		
		final SSConnectionProxy controllingProxy = this;
		shutdownHook = new Thread() {
			@Override
			public void run() {
				try {
					controllingProxy.close();
				} catch (PEException e) {
				}
			}
		};
		ssConnection.addShutdownHook(shutdownHook);
		
		super.initialize();
	}

	@Override
	public void onMessage(Envelope e) throws PEException, PEException {
		throw new PEException(
				"ConnectionManager.onMessage should never be called");
	}

	@Override
	public  void close() throws PEException {
		if ( ssConnection != null ) {
			ssConnection.removeShutdownHook(shutdownHook);
			SSConnection connectionToClose = ssConnection;
			ssConnection = null;
			connectionToClose.close();
			super.close();
			if ( !"RUNTIME".equals(operatingContext))
				proxySet.remove(this);
		}
	}
	
	public  ResponseMessage executeRequest(Object message) throws PEException, PEException {
		if (ssConnection == null)
			throw new PEException("Connection has been closed");
		
		Envelope e = ssConnection.newEnvelope(message).to(ssConnection.getAddress());
		ResponseMessage resp = sendAndReceive(e);
		return resp;
	}

	public void executeAsync(Object message) throws PEException, PEException {
		if (ssConnection == null)
			throw new PEException("Connection has been closed");
		
		Envelope e = ssConnection.newEnvelope(message).to(ssConnection.getAddress());
		send(e);
	}

	public static void setOperatingContext(String context) {
		operatingContext = context;
	}

	public static void checkForLeaks() throws PEException {
		for (SSConnectionProxy proxy : proxySet) {
			throw new PEException(proxySet.size()
					+ " SSConnectionProxy instance(s) are not closed - the operating context of the first one is "
					+ proxy.operatingContextInstance);
		}
	}

	public SSConnection getConnection() {
		return ssConnection;
	}
}