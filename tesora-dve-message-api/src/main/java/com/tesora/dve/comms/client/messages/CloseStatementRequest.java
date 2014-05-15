// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public class CloseStatementRequest extends StatementBasedRequest {

	private static final long serialVersionUID = 1L;

	public CloseStatementRequest() {
	}

	public CloseStatementRequest(String statementId) {
		super(statementId);
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.CLOSE_STATEMENT_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
