// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPComFieldListRequest extends MSPActionBase {
	final static MSPComFieldListRequest INSTANCE = new MSPComFieldListRequest();

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
		// TODO actually implement this method to return the correct
		// response packets
		ctx.channel().write(new MyErrorResponse());
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x04;
	}

}
