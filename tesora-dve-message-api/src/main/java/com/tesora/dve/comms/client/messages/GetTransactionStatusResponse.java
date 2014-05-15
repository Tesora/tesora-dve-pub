// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

public class GetTransactionStatusResponse extends ResponseMessage {

	private static final long serialVersionUID = 1L;
	
	public GetTransactionStatusResponse() {}

	public GetTransactionStatusResponse(boolean inTransaction) {
		this.inTransaction = inTransaction;
	}

	protected boolean inTransaction;
	
	public boolean isInTransaction() {
		return inTransaction;
	}

	public void setInTransaction(boolean inTransaction) {
		this.inTransaction = inTransaction;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.GET_TRANS_STATUS_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
