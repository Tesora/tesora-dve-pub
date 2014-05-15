// OS_STATUS: public
package com.tesora.dve.dbc;

import java.util.HashSet;
import java.util.Set;

public class ServerDBConnectionParameters {

	private String cacheName = null;
	private Set<String> replSlaveIgnoreTblList = new HashSet<String>();

	public String getCacheName() {
		return cacheName;
	}

	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}
	
	public Set<String> getReplSlaveIgnoreTblList() {
		return replSlaveIgnoreTblList;
	}

	public void setReplSlaveIgnoreTblList(Set<String> replSlaveIgnoreTblList) {
		this.replSlaveIgnoreTblList = replSlaveIgnoreTblList;
	}
	
}
