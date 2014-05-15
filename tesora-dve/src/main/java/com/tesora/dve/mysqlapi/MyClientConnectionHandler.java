// OS_STATUS: public
package com.tesora.dve.mysqlapi;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyMessage;

public abstract class MyClientConnectionHandler extends MessageToMessageDecoder<MyMessage> {
	static final Logger logger = Logger.getLogger(MyClientConnectionHandler.class);

	protected MyClientConnectionContext context;

	public MyClientConnectionHandler(MyClientConnectionContext context) {
		this.context = context;
	}
	
	static void closeChannel(Channel ch) {
		if (ch.isActive())
			 ch.close();
	}

	public MyClientConnectionContext getContext() {
		return context;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		logger.log(Level.WARN, context.getName() + " - Unexpected exception from downstream.", cause);
		closeChannel(ctx.channel());
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		context.setCtx(ctx);
		super.handlerAdded(ctx);
	}

}