// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

public class GetDatabaseResponse extends ResponseMessage {

	private static final long serialVersionUID = 1L;
	
	protected String database;

	public GetDatabaseResponse() {}
	
	public GetDatabaseResponse( String database ) {
		setDatabase(database);
	}
	
	public void setDatabase(String database) {
		this.database = database;
	}
	
	public String getDatabase() {
		return this.database;
	}
	
	@Override
	public MessageType getMessageType() {
		return MessageType.GET_DATABASE_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
