package com.tesora.dve.mysqlapi;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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