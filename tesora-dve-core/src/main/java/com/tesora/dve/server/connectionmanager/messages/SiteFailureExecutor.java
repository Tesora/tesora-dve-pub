// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import org.apache.log4j.Logger;

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.groupmanager.SiteFailureMessage;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class SiteFailureExecutor implements AgentExecutor<SSConnection> {

	static Logger logger = Logger.getLogger(SiteFailureExecutor.class);

	@Override
	public ResponseMessage execute(SSConnection ssCon, Object message) throws Throwable {
		if (logger.isDebugEnabled())
			logger.debug(this.getClass().getSimpleName() + " executes on connection " + ssCon.getName() + ": "+ message);
		SiteFailureMessage failoverMessage = (SiteFailureMessage) message;
		ssCon.purgeSiteInstance(failoverMessage.getSiteInstanceId());
		return null;
	}

}
