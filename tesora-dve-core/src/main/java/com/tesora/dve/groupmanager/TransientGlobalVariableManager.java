package com.tesora.dve.groupmanager;

import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class TransientGlobalVariableManager {

	private Map<String,String> localCache = null;
	
	private final ClusterLock variablesLock;
	
	public TransientGlobalVariableManager() {
		variablesLock = GroupManager.getCoordinationServices().getClusterLock("DVE.Global.Variables");
	}
	
	public Map<String,String> getGlobalVariables(SSConnection conn) {
		variablesLock.sharedLock(conn, "reading global variables map");
		Map<String,String> out = readVariables();
		variablesLock.sharedUnlock(conn, "reading global variables map");
		return out;
	}
	
	private Map<String,String> readVariables() {
		if (localCache == null) {
			// go back to the group svc
		}
		return new HashMap<String,String>(localCache);
	}
	
	public void invalidate() {
		localCache = null;
	}
	
	public void set(String varName, String varValue) {
		// set it at the group svc
	}
	
	
}
