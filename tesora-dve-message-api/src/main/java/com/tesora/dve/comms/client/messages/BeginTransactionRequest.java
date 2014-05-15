// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


public class BeginTransactionRequest extends RequestMessage {

	private static final long serialVersionUID = 4869324899665181900L;

	@Override
	public MessageType getMessageType() {
		return MessageType.BEGIN_TRANS_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
