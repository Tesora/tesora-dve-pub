// OS_STATUS: public
package com.tesora.dve.server.messaging;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Agent;
import com.tesora.dve.worker.agent.Envelope;

public class SimpleTestAgent extends Agent {

	public SimpleTestAgent() throws PEException {
		super("SimpleTestAgent");
	}

	@Override
	public void onMessage(Envelope e) throws PEException{
		// TODO Auto-generated method stub
		
	}
}
