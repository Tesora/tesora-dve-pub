// OS_STATUS: public
package com.tesora.dve.server.statistics;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;

public class StatisticsResponse extends ResponseMessage {

	private static final long serialVersionUID = 1L;

	@Override
	public MessageType getMessageType() {
		return MessageType.STAT_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
