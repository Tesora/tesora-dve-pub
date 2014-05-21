package com.tesora.dve.standalone;

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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.parlb.LoadBalancerProxyInitializer;

public class LoadBalancer {

	private static Logger logger = Logger.getLogger(LoadBalancer.class);

	private static final String PORT_PROPERTY = "loadbalancer.port";

	private LoadBalancerProxyInitializer proxyInitializer;
	private int lbPort;

	public LoadBalancer(Properties props) throws Exception {
		proxyInitializer = new LoadBalancerProxyInitializer(props);
		
		lbPort = props.contains(PORT_PROPERTY) ? Integer.parseInt(props.getProperty(PORT_PROPERTY)) : proxyInitializer.getDefaultPort();

		logger.info("Starting Tesora DVE load balancer using:");
		logger.info("... balancer port : " + lbPort);
	}

	public void run() throws Exception {
		EventLoopGroup bossGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("lb-boss"));
		EventLoopGroup workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("lb-worker"));

		ServerBootstrap b = new ServerBootstrap();

		try {
			b.group(bossGroup, workerGroup)
			  .channel(NioServerSocketChannel.class)
			  .childHandler(proxyInitializer)
			  .childOption(ChannelOption.AUTO_READ, false)
			  .bind(lbPort).sync().channel().closeFuture().sync();
		} catch (Throwable e) {
			throw new Exception("Failed to start load balancer on port " + lbPort, e);
		} finally {
//			b.shutdown();
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			proxyInitializer.close();
		}
	}

	public static void main(String[] args) throws Exception {

		Properties props = PEFileUtils.loadPropertiesFile(LoadBalancer.class, PEConstants.CONFIG_FILE_NAME);

		if (args.length == 2 && "-port".equalsIgnoreCase(args[0]))
			props.setProperty(PORT_PROPERTY, args[1]);
		else if (args.length > 0)
			throw new Exception("Usage: LoadBalancer [-port <port>]");

		InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
		LoadBalancer loadBalancer = new LoadBalancer(props);
		loadBalancer.run();
	}
}
