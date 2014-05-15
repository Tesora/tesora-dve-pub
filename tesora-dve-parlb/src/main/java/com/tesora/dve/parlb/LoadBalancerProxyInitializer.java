// OS_STATUS: public
package com.tesora.dve.parlb;

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
