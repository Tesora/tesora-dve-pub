// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

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
