package com.tesora.dve.groupmanager;

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
