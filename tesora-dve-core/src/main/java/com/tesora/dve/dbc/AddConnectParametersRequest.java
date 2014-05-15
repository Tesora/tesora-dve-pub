// OS_STATUS: public
package com.tesora.dve.dbc;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.RequestMessage;

public class AddConnectParametersRequest extends RequestMessage {
	private static final long serialVersionUID = 1L;

	private ServerDBConnectionParameters svrDBParams;

	public AddConnectParametersRequest(ServerDBConnectionParameters svrDBParams) {
		this.svrDBParams = svrDBParams;
	}

	public ServerDBConnectionParameters getSvrDBParams() {
		return svrDBParams;
	}

	public void setSvrDBParams(ServerDBConnectionParameters svrDBParams) {
		this.svrDBParams = svrDBParams;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.ADD_CONNECT_PARAMETERS_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}
