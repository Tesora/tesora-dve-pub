// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public class RollbackTransactionRequest extends RequestMessage {

	private static final long serialVersionUID = -4098483156352431749L;

	@Override
	public MessageType getMessageType() {
		return MessageType.END_TRANS_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
