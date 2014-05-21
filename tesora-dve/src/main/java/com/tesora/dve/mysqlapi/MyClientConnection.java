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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.MysqlConnection;

public class MyClientConnection {
	private static final Logger logger = Logger.getLogger(MyClientConnection.class);

	private Channel channel = null;
	private EventLoopGroup workerGroup = null;
	private MyClientConnectionContext context = null;
	
	protected boolean connected = false;
	
	// This constructor is used if the caller wants the ChannelFactory managed
	// for them
	public MyClientConnection(MyClientConnectionContext context) {
		this.context = context;
	}

	public boolean start(final ChannelHandlerAdapter responseHandler) {
		boolean ret = false;

		workerGroup = new NioEventLoopGroup();
		ChannelFuture f = null;
		try {
			Bootstrap b = new Bootstrap();
			b.group(workerGroup);
			b.channel(NioSocketChannel.class);
			b.option(ChannelOption.SO_REUSEADDR, true);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			b.option(ChannelOption.ALLOCATOR,
					MysqlConnection.USE_POOLED_BUFFERS ? PooledByteBufAllocator.DEFAULT
							: UnpooledByteBufAllocator.DEFAULT);
			b.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline().addLast(MyEncoder.class.getSimpleName(),
							new MyEncoder());
					ch.pipeline().addLast(
							MyClientSideSyncDecoder.class.getSimpleName(),
							new MyClientSideSyncDecoder());
					ch.pipeline().addLast(
							responseHandler.getClass().getSimpleName(),
							responseHandler);
				}
			});

			// Start the client.
			f = b.connect(context.getConnectHost(), context.getConnectPort())
					.sync();
			if (f != null) {
				channel = f.channel();
				ret = isStarted();
			}

		} catch (Exception e) {
			stop();
		}

		connected = false;

		return ret;
	}

	public void stop() {
		if (workerGroup != null) {
			if (!workerGroup.isShuttingDown()) {
				workerGroup.shutdownGracefully();
				workerGroup = null;
			}
		}
	}

	public boolean isStarted() {
		if (channel == null)
			return false;

		return channel.isActive();
	}

	public boolean isConnected() {
		if (!isStarted()) {
			return false;
		}
		return connected;
	}
	
	public MyClientConnectionContext getContext() {
		return context;
	}

	public void changeToAsyncPipeline(MyDecoder newDecoder,
			MyClientConnectionHandler newHandler) {
		if (logger.isDebugEnabled())
			logger.debug("Switching MyClientConnection to asynchronous mode");

		channel.pipeline().addFirst(newDecoder.getClass().getSimpleName(),
				newDecoder);
		channel.pipeline()
				.remove(MyClientSideSyncDecoder.class.getSimpleName());
		channel.pipeline().addLast(newHandler.getClass().getSimpleName(),
				newHandler);
		connected = true;
	}
}
