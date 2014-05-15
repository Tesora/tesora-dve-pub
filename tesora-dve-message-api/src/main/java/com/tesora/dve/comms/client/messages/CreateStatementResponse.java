// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

public class CreateStatementResponse extends ResponseMessage {

	private static final long serialVersionUID = 8312275422824972802L;

	protected String statementID;

	public CreateStatementResponse(String statementId) {
		setStatementId(statementId);
	}

	public CreateStatementResponse() {
	}

	public void setStatementId(String stmtId) {
		this.statementID = stmtId;
	}

	public String getStatementId() {
		return statementID;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.CREATE_STATEMENT_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
