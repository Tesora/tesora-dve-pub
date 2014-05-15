// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPComPingRequest extends MSPActionBase {

	final static MSPComPingRequest INSTANCE = new MSPComPingRequest();
	
	private static final Logger logger = Logger.getLogger(MSPComPingRequest.class);

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
		if (logger.isDebugEnabled())
			logger.debug(ctx.channel().toString() + " - Ping request recieved - returning OK response");

		ctx.channel().write(new MyOKResponse());
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x0e;
	}

}
