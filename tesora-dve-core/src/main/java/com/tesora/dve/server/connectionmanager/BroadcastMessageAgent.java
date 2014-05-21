// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.server.connectionmanager.log.ShutdownLog;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.PurgeWorkerGroupCaches;
import com.tesora.dve.server.connectionmanager.messages.AgentExecutor;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.worker.agent.Agent;

public class BroadcastMessageAgent extends Agent {

	Logger logger = Logger.getLogger(BroadcastMessageAgent.class);

	static Map<String, AgentExecutor<BroadcastMessageAgent>> executorMap = new HashMap<String, AgentExecutor<BroadcastMessageAgent>>() {
		private static final long serialVersionUID = 1L;
		{
			put(PurgeWorkerGroupCaches.class.getName(), PurgeWorkerGroupFactoryExecutor.INSTANCE);
		}
	};
	
	public BroadcastMessageAgent() throws PEException {
		super("BroadcastMessageAgent");
	}

	@Override
	public void onMessage(Envelope e) throws PEException {
		try {
			if (executorMap.containsKey(e.getPayload().getClass().getName())) {
				AgentExecutor<BroadcastMessageAgent> agentExec = executorMap.get(e.getPayload().getClass().getName());
				agentExec.execute(this, e.getPayload());
			} else {
				throw new PEException("Message has no BroadcastExecutor: " + e);
			}
		} catch (Throwable re) {
			logger.warn("Exception executing broadcast message " + e);
		}
	}

	public void shutdown() {
		try {
			super.close();
		} catch (PEException e) {
			ShutdownLog.logShutdownError("Error shutting down " + getClass().getSimpleName(), e);
		}
	}

}
