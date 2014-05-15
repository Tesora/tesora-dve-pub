// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public class ExecuteRequest extends QueryStatementBasedRequest {

	private static final long serialVersionUID = 1L;

	public ExecuteRequest() {
	};

	public ExecuteRequest(String statementId, String command) {
		super(statementId, command);
	}

	public ExecuteRequest(String statementId, byte[] command) {
		super(statementId, command);
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.EXECUTE_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
