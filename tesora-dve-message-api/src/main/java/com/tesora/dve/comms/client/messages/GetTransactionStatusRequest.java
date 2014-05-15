// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public class GetTransactionStatusRequest extends RequestMessage {

	private static final long serialVersionUID = 1L;

	@Override
	public MessageType getMessageType() {
		return MessageType.GET_TRANS_STATUS_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
