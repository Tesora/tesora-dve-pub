// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

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
