package com.tesora.dve.db.mysql.portal;

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

import com.tesora.dve.db.mysql.portal.protocol.MSPEncoder;
import com.tesora.dve.db.mysql.portal.protocol.MSPProtocolDecoder;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.global.MySqlPortalService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.variables.KnownVariables;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.tesora.dve.concurrent.PEDefaultThreadFactory;
import com.tesora.dve.concurrent.PEThreadPoolExecutor;
import com.tesora.dve.exceptions.PEException;

public class MySqlPortal implements MySqlPortalService {
	
	private static final boolean USE_POOLED_BUFFERS = Boolean.getBoolean("com.tesora.dve.netty.usePooledBuffers");
	private static final boolean PACKET_LOGGER = Boolean.getBoolean("MysqlPortal.packetLogger");

	private static final Logger logger = Logger.getLogger(MySqlPortal.class);
	private static MySqlPortal portal;
	
	private NioEventLoopGroup bossGroup;
	private NioEventLoopGroup workerGroup;

	private ThreadPoolExecutor clientExecutorService;

	public MySqlPortal(Properties props) throws PEException {
		// This is the port the Portal is going to listen on -
		// default to Mysql's port
        int port = Singletons.require(HostService.class).getPortalPort(props);
        Singletons.replace(MySqlPortalService.class,this);

		InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

		int max_concurrent = KnownVariables.MAX_CONCURRENT.getValue(null).intValue();

        //TODO: parse/plan is on this pool, which is probably ok, especially with blocking calls to catalog.  Check for responses that can be done by backend netty threads and avoid two context shifts.

		clientExecutorService = new PEThreadPoolExecutor(max_concurrent,
				max_concurrent,
				30L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(),  //The thread count limits concurrency here.  Using a bounded queue here would block netty threads (very bad), so this pool could be overrun by 'bad' clients that pipeline. -sgossard
				new PEDefaultThreadFactory("msp-client"));
		clientExecutorService.allowCoreThreadTimeOut(true);
		
		bossGroup = new NioEventLoopGroup(1, new PEDefaultThreadFactory("msp-boss"));

        //fixes the number of Netty NIO threads to the number of available CPUs.
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(), new PEDefaultThreadFactory("netty-worker"));

		ServerBootstrap b = new ServerBootstrap();
		try {
			b.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.childHandler(new ChannelInitializer<SocketChannel>() {

                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    if (PACKET_LOGGER)
                        ch.pipeline().addFirst(new LoggingHandler(LogLevel.INFO));
                    ch.pipeline()
                            .addLast(MSPEncoder.getInstance())
                            .addLast(MSPProtocolDecoder.class.getSimpleName(), new MSPProtocolDecoder(MSPProtocolDecoder.MyDecoderState.READ_CLIENT_AUTH))
                            .addLast(new MSPAuthenticateHandlerV10())
                            .addLast(MSPCommandHandler.class.getSimpleName(), new MSPCommandHandler(clientExecutorService))
                            .addLast(ConnectionHandlerAdapter.getInstance());
                }
            })
                    
			.childOption(ChannelOption.ALLOCATOR, USE_POOLED_BUFFERS ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT)
			.childOption(ChannelOption.TCP_NODELAY, true)
			.childOption(ChannelOption.SO_KEEPALIVE, true)
			.bind(port).sync();

			logger.info("DVE Server bound to port " + port);

		} catch (Exception e) {
			throw new PEException("Failed to bind DVE server to port " + port + " - " + e.getMessage(), e);
		} 				
	}

    void releaseResources() {
		if ( bossGroup != null )
			bossGroup.shutdownGracefully();
		if ( workerGroup != null ) 
			workerGroup.shutdownGracefully();
		if ( clientExecutorService != null )
			clientExecutorService.shutdown();
	}

	public static void start(Properties props) throws PEException {
		portal = new MySqlPortal(props);

		if (logger.isDebugEnabled())
			logger.debug("MySqlPortal started...");
	}
	
	public static void stop() {
		if (portal != null) {
			portal.releaseResources();
			portal = null;
		}
	}

    @Override
    public void setMaxConcurrent(int maxConcurrent) {
		if(maxConcurrent == 0) 
			return;
		
		if(clientExecutorService != null) {
			clientExecutorService.setCorePoolSize(maxConcurrent);
			clientExecutorService.setMaximumPoolSize(maxConcurrent);
		}
	}
	
	@Override
    public int getWorkerGroupCount() {
		return workerGroup.executorCount();
	}
	
	@Override
    public int getWorkerExecGroupCount() {
		return 0;
	}
	
	@Override
    public int getClientExecutorActiveCount() {
		return clientExecutorService.getActiveCount();
	}

	@Override
    public int getClientExecutorCorePoolSize() {
		return clientExecutorService.getCorePoolSize();
	}

	@Override
    public int getClientExecutorPoolSize() {
		return clientExecutorService.getPoolSize();
	}

	@Override
    public int getClientExecutorLargestPoolSize() {
		return clientExecutorService.getLargestPoolSize();
	}

	@Override
    public int getClientExecutorMaximumPoolSize() {
		return clientExecutorService.getMaximumPoolSize();
	}
	
	@Override
    public int getClientExecutorQueueSize() {
		return clientExecutorService.getQueue().size();
	}
}
