// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

interface MSPAction {
	void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx, SSConnection ssCon, MSPMessage protocolMessage) throws PEException;
	byte getMysqlMessageType();
}
