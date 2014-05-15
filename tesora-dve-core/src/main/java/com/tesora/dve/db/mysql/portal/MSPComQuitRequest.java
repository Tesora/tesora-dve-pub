// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;


import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPComQuitRequest extends MSPActionBase {

	public static final MSPComQuitRequest INSTANCE = new MSPComQuitRequest();

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
		ctx.channel().close();
	}

    @Override
	public byte getMysqlMessageType() {
		return (byte) 0x01;
	}
}
