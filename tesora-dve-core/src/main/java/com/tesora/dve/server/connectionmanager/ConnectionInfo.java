// OS_STATUS: public
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

import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class ConnectionInfo {
	private int connectionId;
	private String user;
	private String host;
	private String db;
	private String command;
	private long time;
	private String state;
	private String info;

	private Map</* site */String, /* connId */Integer> siteConnections = new HashMap<String, Integer>();

	public ConnectionInfo(int connectionId, String user) {
		this.connectionId = connectionId;
        this.host = Singletons.require(HostService.class).getHostName() + ":0";
		this.user = user;
		resetConnectionState();
	}

	public int getConnectionId() {
		return connectionId;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}
	
	public String getHost() {
		return host;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}
	
	public String getCommand() {
		return command;
	}

	public long getTimeinState() {
		return (System.currentTimeMillis() - time)/1000;
	}

	public String getState() {
		return state;
	}

	public String getInfo() {
		return info;
	}

	public void changeConnectionState(String command, String state, String info) {
		this.command = command;
		this.state = state;
		this.info = info;
		this.time = System.currentTimeMillis(); 
	}
	
	public void resetConnectionState() {
		this.command = null;
		this.state = "Sleep";
		this.info = null;
		this.time = System.currentTimeMillis();
	}

	public void registerSiteConnection(StorageSite site, int siteConnectionId) {
		siteConnections.put(site.getName(), siteConnectionId);
	}

	public void unregisterSiteConnection(StorageSite site) {
		siteConnections.remove(site.getName());
	}

	public int getSiteConnectionId(StorageSite site) {
		Integer connId = siteConnections.get(site.getName());
		return connId != null ? connId.intValue() : 0;
	}

}
