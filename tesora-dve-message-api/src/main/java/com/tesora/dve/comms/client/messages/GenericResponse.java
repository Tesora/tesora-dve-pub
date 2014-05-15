// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

public class GenericResponse extends ResponseMessage {

	private static final long serialVersionUID = -5704487672804338259L;
	
	public GenericResponse() {
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.GENERIC_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}
}
