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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.dbc.AddConnectParametersRequest;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class AddConnectParametersExecutor implements AgentExecutor<SSConnection> {
	Logger logger = Logger.getLogger(AddConnectParametersExecutor.class);

	@Override
	public ResponseMessage execute(SSConnection agent, Object message) throws Throwable {

		AddConnectParametersRequest acpr = (AddConnectParametersRequest) message;
		
		for ( String qualifiedTable : acpr.getSvrDBParams().getReplSlaveIgnoreTblList() ) {
			String[] tableComponents = StringUtils.split(qualifiedTable, '.');
			if ( tableComponents.length != 2 )
				logger.warn("Invalid entry '" + qualifiedTable + "' in Replication Slave table ignore list");
			else 
				if ( logger.isDebugEnabled() )
					logger.debug("Adding " + qualifiedTable + " to table list");

			agent.getReplicationOptions().addTableFilter(agent.getSchemaContext(), tableComponents[0], tableComponents[1]);
		}
		
		agent.setCacheName(acpr.getSvrDBParams().getCacheName());
		
		return new GenericResponse().success();
	}

}
