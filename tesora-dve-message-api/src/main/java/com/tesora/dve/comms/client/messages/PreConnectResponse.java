// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

import com.tesora.dve.common.DBType;

public abstract class PreConnectResponse extends ResponseMessage {

	private static final long serialVersionUID = 1L;
	
	protected DBType dbType;
	protected int connectID;
	protected String serverVersion;
	
	public void setDBType( DBType dbType ) {
		this.dbType = dbType;
	}
	
	public DBType getDBType() {
		return this.dbType;
	}
	
	public void setConnectId( int connectId ) {
		this.connectID = connectId;
	}
	
	public int getConnectId() {
		return this.connectID;
	}
	
	public void setServerVersion(String serverVersion) {
		this.serverVersion = serverVersion;
	}

	public String getServerVersion() {
		return this.serverVersion;
	}
}
