// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPComProcessInfoRequest extends MSPActionBase {
	
	final static MSPComProcessInfoRequest INSTANCE = new MSPComProcessInfoRequest(); 

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
        byte sequenceId = protocolMessage.getSequenceID();
		MyErrorResponse errMsg = new MyErrorResponse(new PEException(
				"COM_PROCESS_INFO message has been deprecated - use SHOW PROCESSLIST"));
		errMsg.setPacketNumber(sequenceId + 1);
		ctx.channel().write(errMsg);
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x0a;
	}

}
