// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public class DBMetadataRequest extends MetadataRequest  {

	private static final long serialVersionUID = 1L;

	public DBMetadataRequest(String stmtId) {
		super(stmtId);
	}

	public DBMetadataRequest() {
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.DBMETADATA_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}
}
