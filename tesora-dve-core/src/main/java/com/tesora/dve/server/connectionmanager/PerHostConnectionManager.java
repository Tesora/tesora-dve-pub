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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.comms.client.messages.ClientMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.StatementManager;
import com.tesora.dve.worker.agent.Agent;

public enum PerHostConnectionManager {
	
	INSTANCE;
	
	AtomicLong totalConnections = new AtomicLong(0);
	AtomicLong maxConcurrentConnections = new AtomicLong(0);
	AtomicLong totalConnectFailures = new AtomicLong(0);
	AtomicLong totalClientFailures = new AtomicLong(0);
	
	ConcurrentMap<Integer, SSConnection> connectionIdMap = new ConcurrentHashMap<Integer, SSConnection>();
	ConcurrentMap<Integer, ConnectionInfo> connectionInfoMap = new ConcurrentHashMap<Integer, ConnectionInfo>();
	
	void registerConnection(int connectionId, SSConnection ssCon) {
        connectionIdMap.put(connectionId, ssCon);
		connectionInfoMap.put(connectionId, new ConnectionInfo(connectionId, "user"));
		incrementCounters();
	}
	
	void unRegisterConnection(int connectionId) {
		connectionIdMap.remove(connectionId);
		connectionInfoMap.remove(connectionId);
	}
	
	public void closeAllConnections() throws PEException {
		StatementManager.INSTANCE.cancellAllConnections();
		
		for (SSConnection ssConnection : connectionIdMap.values())
			ssConnection.close();
	}
	
	public void closeAllConnectionsWithUserlandTemporaryTables() throws PEException {
		List<SSConnection> copied = new ArrayList<SSConnection>(connectionIdMap.values());
		for(SSConnection ssconn : copied) {
			if (ssconn.hasUserlandTemporaryTables()) {
				StatementManager.INSTANCE.cancelAllStatements(ssconn.getConnectionId());
				ssconn.close();
			}
		}
	}
	
	public void clear() {
		connectionIdMap.clear();
		connectionInfoMap.clear();
	}

	public void closeConnection(int connectionId) throws PEException {
		if (connectionIdMap.containsKey(connectionId)) {
			SSConnection ssConnection = connectionIdMap.get(connectionId);
			connectionIdMap.remove(connectionId);
			connectionInfoMap.remove(connectionId);
			ssConnection.close();
		}
	}
	
	public void setUserInfo(int connId, String user) {
		connectionInfoMap.get(connId).setUser(user);
	}
	
	public void setConnectionDB(int connId, String db) {
		if (connectionInfoMap.containsKey(connId)) {
			ConnectionInfo connectionInfo = connectionInfoMap.get(connId);
			connectionInfo.setDb(db);
		}
	}
	
	public void changeConnectionState(int connId, String command, String state, String info) {
		if (connectionInfoMap.containsKey(connId)) {
			ConnectionInfo connectionInfo = connectionInfoMap.get(connId);
			connectionInfo.changeConnectionState(command, state, info);
		}
	}

	public void resetConnectionState(int connId) {
		if (connectionInfoMap.containsKey(connId)) {
			ConnectionInfo connectionInfo = connectionInfoMap.get(connId);
			connectionInfo.resetConnectionState();
		}
	}

	public Collection<ConnectionInfo> getConnectionInfoList() {
		List<ConnectionInfo> sortedList = new ArrayList<ConnectionInfo>(connectionInfoMap.values());
		Collections.sort(sortedList, new Comparator<ConnectionInfo>(){
			@Override
			public int compare(ConnectionInfo ci1, ConnectionInfo ci2) {
				return ci1.getConnectionId() - ci2.getConnectionId();
			}
		});
		
		return sortedList;
	}
	
	public void printProcessList() {
		for (ConnectionInfo info : connectionInfoMap.values())
			System.out.println(info.getConnectionId() + "\t" + info.getUser() + "\t" + info.getDb() + "\t" + info.getInfo());
	}

	public void sendToAllConnections(ClientMessage message) throws PEException {
		for (SSConnection ssConnection : connectionIdMap.values()) {
			Agent.dispatch(ssConnection.getAddress(), message);
		}
	}

	private void incrementCounters() {
		totalConnections.incrementAndGet();
		int currentConns = getConnectionCount();
		if ( currentConns > maxConcurrentConnections.get() ) {
			maxConcurrentConnections.set(currentConns);
		}
	}
	
	public void addConnectFailure() {
		totalConnectFailures.incrementAndGet();
	}
	
	public long getTotalConnectionFailures() {
		return totalConnectFailures.get();
	}
	
	public void resetTotalConnectionFailures()  {
		totalConnectFailures.set(0);
	}
	
	public int getConnectionCount() {
		return connectionIdMap.size();
	}

	public SSConnection lookupConnection(final Integer connectionId) {
		return connectionIdMap.get(connectionId);
	}

	public long getTotalConnections() {
		return totalConnections.get();
	}

	public void resetTotalConnections() {
		totalConnections.set(0);
	}

	public void addClientFailure() {
		totalClientFailures.incrementAndGet();
	}
	
	public long getTotalClientFailures() {
		return totalClientFailures.get();
	}

	public void resetTotalClientFailures() {
		totalClientFailures.set(0);
	}
	
	public long getMaxConcurrentConnections() {
		return maxConcurrentConnections.get();
	}

	public void resetMaxConcurrentConnections() {
		maxConcurrentConnections.set(0);
	}

	public void registerSiteConnection(int connId, StorageSite site, int siteConnId) {
        if (connectionInfoMap.containsKey(connId)) {
			ConnectionInfo connectionInfo = connectionInfoMap.get(connId);
			connectionInfo.registerSiteConnection(site, siteConnId);
		}
	}

	public void unregisterSiteConnection(int connId, StorageSite site) {
		if (connectionInfoMap.containsKey(connId)) {
			ConnectionInfo connectionInfo = connectionInfoMap.get(connId);
			connectionInfo.unregisterSiteConnection(site);
		}
	}

	public ConnectionInfo getConnectionInfo(int connId) {
		return connectionInfoMap.get(connId);
	}

}
