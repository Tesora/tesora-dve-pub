package com.tesora.dve.parlb;

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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PECollectionUtils;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.hazelcast.HazelcastGroupClient;

public class LoadBalancerProxyInitializer extends ChannelInitializer<SocketChannel> {

	private static Logger logger = Logger.getLogger(LoadBalancerProxyInitializer.class);

	HazelcastGroupClient hazelcastClient;

	public LoadBalancerProxyInitializer(Properties props) throws Exception {
		this.hazelcastClient = new HazelcastGroupClient();
		hazelcastClient.startHazelcastClient(props);
	}

	public void close() {
		hazelcastClient.stopHazelcastClient();
	}

	@Override
	public void initChannel(SocketChannel ch) throws Exception {
		if (hazelcastClient.isConnected()) {
			InetSocketAddress selectedMember = PECollectionUtils.selectRandom(hazelcastClient.getMembers());
			InetSocketAddress selectedPEServer = hazelcastClient.getPEServerAddress(selectedMember);
			logger.debug("Selected PE server: " + selectedPEServer);
			ch.pipeline().addLast(
					// new LoggingHandler(LogLevel.INFO),
					new MysqlClientHandler(selectedPEServer));
		} else {
			throw new PEException("Not connected to DVE server group.");
		}
	}

	public int getDefaultPort() {
		return PEConstants.MYSQL_PORT;
	}
}
