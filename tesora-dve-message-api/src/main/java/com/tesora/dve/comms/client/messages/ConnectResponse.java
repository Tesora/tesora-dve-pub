// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

public class ConnectResponse extends ResponseMessage {
	private static final long serialVersionUID = 1L;

	protected String connectionID;
	
	public ConnectResponse(String connectionID) {
		this.connectionID = connectionID;
	}

	public ConnectResponse() {
	}

	public String getConnectionID() {
		return connectionID;
	}

	public void setConnectionID(String connectionID) {
		this.connectionID = connectionID;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.CONNECT_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
