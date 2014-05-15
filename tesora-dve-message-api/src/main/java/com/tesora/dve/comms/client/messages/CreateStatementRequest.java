// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public class CreateStatementRequest extends RequestMessage {

	private static final long serialVersionUID = 5238718931573359287L;

	@Override
	public MessageType getMessageType() {
		return MessageType.CREATE_STATEMENT_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
