// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

import com.tesora.dve.comms.client.messages.RequestMessage;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Envelope;

@SuppressWarnings("serial")
public abstract class NotificationManagerRequest extends RequestMessage {

	public abstract ResponseMessage executeRequest(Envelope e, NotificationManager nm) throws PEException;
}
