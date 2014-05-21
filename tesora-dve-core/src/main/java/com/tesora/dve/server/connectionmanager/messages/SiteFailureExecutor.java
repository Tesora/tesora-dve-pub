// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

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
