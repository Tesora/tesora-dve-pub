// OS_STATUS: public
package com.tesora.dve.sql.schema;

public class UserScope {

	private final String username;
	private final String scope;
	
	public UserScope(String name, String machineScope) {
		username = name;
		scope = machineScope;
	}
	
	public String getUserName() { return username; }
	public String getScope() { return scope; }
	
	public String getSQL() {
		return "'" + username + "'@'" + scope + "'";
	}
	
}
