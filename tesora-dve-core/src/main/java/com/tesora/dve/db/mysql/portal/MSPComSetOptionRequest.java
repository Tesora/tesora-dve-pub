// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPComSetOptionRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPComSetOptionRequest extends MSPActionBase {
	static final Logger logger = Logger.getLogger(MSPComSetOptionRequest.class);
	protected static final MSPAction INSTANCE = new MSPComSetOptionRequest();

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {

        MSPComSetOptionRequestMessage stmtCloseMessage = castProtocolMessage(MSPComSetOptionRequestMessage.class,protocolMessage);

		short option = stmtCloseMessage.getOptionFlag();
		logger.warn("Recieved COM_SET_OPTION with option value of " + option);
		ctx.channel().write(new MyOKResponse());
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x1b;
	}
}
