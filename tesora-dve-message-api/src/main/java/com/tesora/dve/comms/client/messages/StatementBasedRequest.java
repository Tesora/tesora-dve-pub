// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

public abstract class StatementBasedRequest extends RequestMessage {

	private static final long serialVersionUID = 1L;

	protected String statementID;
	
	protected StatementBasedRequest() {
	};

	protected StatementBasedRequest(String stmtId) {
		this.setStatementId(stmtId);
	}

	public String getStatementId() {
		return this.statementID;
	}

	public void setStatementId(String stmtId) {
		this.statementID = stmtId;
	}
}
