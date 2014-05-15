// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPServerGreetingRequest extends MSPActionBase {
	
	final static MSPServerGreetingRequest INSTANCE = new MSPServerGreetingRequest();

    @Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
		throw new PECodingException();
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0xD0;
	}


}
