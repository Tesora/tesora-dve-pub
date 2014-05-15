// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

import java.io.Serializable;

public abstract class ClientMessage implements Serializable {
	private static final long serialVersionUID = 1L;

	// ---------
	// classes extending ClientMessage should implement these methods to return
	// the relevant values.
	public abstract MessageType getMessageType();

	public abstract MessageVersion getVersion();

	// -------

	// these are callbacks that can be used to perform message specific
	// processing before message encoding or after message decoding
	public void unmarshallMessage() {
	}

	public void marshallMessage() {
	}

	@Override
	public String toString() {
		return new StringBuilder().append(getClass().getSimpleName()).append("{").append("MessageVersion=")
				.append(this.getVersion()).append(", MessageType=").append(this.getMessageType()).append(" elements")
				.append('}').toString();
	}
}
