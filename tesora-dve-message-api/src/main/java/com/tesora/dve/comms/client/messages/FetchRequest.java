// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public class FetchRequest extends StatementBasedRequest {

	private static final long serialVersionUID = 1L;

	public FetchRequest() {
	}

	public FetchRequest(String statementId) {
		super(statementId);
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.FETCH_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}


}
