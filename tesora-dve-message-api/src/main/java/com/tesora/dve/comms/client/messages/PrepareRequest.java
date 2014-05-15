// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

public class PrepareRequest extends QueryStatementBasedRequest {

	private static final long serialVersionUID = 1L;

	public PrepareRequest() {
	};

	public PrepareRequest(String statementId, String command) {
		super(statementId, command);
	}

	public PrepareRequest(String statementId, byte[] command) {
		super(statementId, command);
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.PREPARE_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}
}
