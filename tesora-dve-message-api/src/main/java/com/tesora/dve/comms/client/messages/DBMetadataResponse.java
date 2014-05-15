// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

import com.tesora.dve.db.DBMetadata;

public class DBMetadataResponse extends ResponseMessage {
	private static final long serialVersionUID = 1L;
	
	protected DBMetadata dbMetadata;
	
	public DBMetadataResponse() {
	}

	public DBMetadataResponse(DBMetadata dbMetadata) {
		this.dbMetadata = dbMetadata;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.DBMETADATA_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

	public DBMetadata getDbMetadata() {
		return dbMetadata;
	}

	public void setDbMetadata(DBMetadata dbMetadata) {
		this.dbMetadata = dbMetadata;
	}

}
