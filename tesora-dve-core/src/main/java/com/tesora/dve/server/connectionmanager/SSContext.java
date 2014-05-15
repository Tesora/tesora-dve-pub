// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

public class SSContext {
	
	int connectionId;
	String transactionId;
	
	public SSContext(int connectionId, String transactionId) {
		this.connectionId = connectionId;
		this.transactionId = transactionId;
	}
	public SSContext(int connectionId) {
		this(connectionId, null);
	}
	
	public int getConnectionId() {
		return connectionId;
	}
	public String getTransId() {
		return transactionId;
	}
	public void clearTransactionId() {
		transactionId = null;
	}
	
}
