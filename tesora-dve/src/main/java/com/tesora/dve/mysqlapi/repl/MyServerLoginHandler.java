// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.messages.MyComRegisterSlaveRequest;

public class MyServerLoginHandler extends MessageToMessageDecoder<MyMessage> {
	private static final Logger logger = Logger
			.getLogger(MyServerLoginHandler.class);

	MyReplicationSlaveService plugin;
	MyReplSlaveClientConnection myClient;

	public MyServerLoginHandler(MyReplicationSlaveService plugin,
			MyReplSlaveClientConnection myClient) {
		this.plugin = plugin;
		this.myClient = myClient;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, MyMessage msg,
			List<Object> out) throws Exception {
		try {
			onLogin(ctx, msg);
		} finally {
			ctx.pipeline().remove(this);
		}
	}

	void onLogin(ChannelHandlerContext ctx, MyMessage rm) throws PEException {
		if (!(((MyResponseMessage) rm).isOK())) {
			myClient.stop();
			throw new PEException(((MyErrorResponse) rm).getErrorMsg());
		}

		if (logger.isDebugEnabled())
			logger.debug("Successful login to " + plugin.getMasterLocator()
					+ " as " + plugin.getMasterUserid());

		slaveRegisterRequest(ctx, plugin.getSlaveServerID(),
				plugin.getMasterPort());
	}

	void slaveRegisterRequest(ChannelHandlerContext ctx, int slaveID,
			int masterPort) throws PEException {

		if (logger.isDebugEnabled())
			logger.debug("Sending slave registration request to "
					+ plugin.getMasterLocator() + " for Slave #"
					+ plugin.getSlaveServerID());

		MyComRegisterSlaveRequest rsr = new MyComRegisterSlaveRequest(slaveID, masterPort);
		ctx.writeAndFlush(rsr);

		ctx.pipeline().addLast(
				MyServerSlaveRegisterHandler.class.getSimpleName(),
				new MyServerSlaveRegisterHandler(plugin, myClient));
	}

}
