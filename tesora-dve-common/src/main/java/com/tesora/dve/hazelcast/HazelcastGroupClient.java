// OS_STATUS: public
package com.tesora.dve.hazelcast;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

public class HazelcastGroupClient extends HazelcastGroupMember implements LifecycleListener, MembershipListener {

	static Logger logger = Logger.getLogger(HazelcastGroupClient.class);

	private HazelcastClient ourHazelcastInstance;
	private Properties props;
	private boolean autoReconnect = true;
	private boolean waitForServers = true;

	public HazelcastGroupClient() {

	}

	public void startHazelcastClient(Properties props) throws Exception {
		this.props = props;
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName(HAZELCAST_GROUP_NAME).setPassword(HAZELCAST_GROUP_PASSWORD);
		List<String> servers = waitForServers(props);
		clientConfig.setAddresses(servers);
		clientConfig.setInitialConnectionAttemptLimit(3);
		clientConfig.setReconnectionAttemptLimit(3);

		ourHazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
		ourHazelcastInstance.getLifecycleService().addLifecycleListener(this);
		ourHazelcastInstance.getCluster().addMembershipListener(this);
		logClusterMembers();
	}

	public void stopHazelcastClient() {
		autoReconnect = false;
		waitForServers = false;
		cleanup();
	}

	public boolean isConnected() {
		try {
			return getOurHazelcastInstance().getLifecycleService().isRunning();
		} catch (Throwable t) {
			logger.debug("Group client not connected", t);
			return false;
		}
	}

	@Override
	protected HazelcastInstance getOurHazelcastInstance() {
		if (ourHazelcastInstance == null) {
			throw new IllegalStateException("Client is not connected.");
		}
		return ourHazelcastInstance;
	}

	@Override
	public void memberAdded(MembershipEvent event) {
		logClusterMembers();
	}

	@Override
	public void memberRemoved(MembershipEvent event) {
		logClusterMembers();
	}

	@Override
	public void stateChanged(LifecycleEvent event) {
		switch (event.getState()) {
		case CLIENT_CONNECTION_OPENED:
			logger.info("Connected to DVE cluster.");
			break;
		case CLIENT_CONNECTION_LOST:
			logger.info("Disconnected from DVE cluster.");
			break;
		case SHUTDOWN:
			cleanup();
			if (autoReconnect) {
				reconnect();
			}
			break;
		default:
			// logger.debug(event);
		}

	}

	private void reconnect() {
		Thread t = new Thread() {
			public void run() {
				logger.info("Attempting to reconnect...");
				while (autoReconnect && !isConnected()) {
					try {
						startHazelcastClient(props);
					} catch (Exception e) {
						logger.debug("Unable to reconnect to cluster", e);
					}
				}
			}
		};
		t.setName("parlb-client-reconnect");
		t.start();
	}

	private void logClusterMembers() {
		List<InetSocketAddress> members = getMembers();
		logger.info("Available servers: " + members.size());
		if (!members.isEmpty()) {
			for (InetSocketAddress address : members) {
				logger.info("... " + address);
			}
		}
	}

	private List<String> waitForServers(Properties props) throws Exception {
		Connection con = null;
		List<String> servers = new ArrayList<String>();
		try {
			con = getDBConnection(props);
			servers = findAllRegisteredServers(con);
			if (servers.isEmpty() && waitForServers) {
				logger.info("No registered servers. Waiting...");
				for (int attempt = 0; waitForServers && servers.isEmpty(); attempt++) {
					// try for a minute, then back off
					Thread.sleep(attempt < 12 ? 5000 : 30000);
					servers = findAllRegisteredServers(con);
				}
				logger.info("Found " + servers.size() + " registered server(s).");
				logger.debug("Registered servers: " + servers);
			}
		} finally {
			if (con != null)
				con.close();
		}
		return servers;
	}

	private void cleanup() {
		if (ourHazelcastInstance != null) {
			ourHazelcastInstance.getCluster().removeMembershipListener(this);
			ourHazelcastInstance.getLifecycleService().removeLifecycleListener(this);
			ourHazelcastInstance.getLifecycleService().shutdown();
		}
		ourHazelcastInstance = null;
	}

}
