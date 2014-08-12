package com.tesora.dve.groupmanager;

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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.lockmanager.LockManager;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.Join;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapConfig.StorageType;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.externalservice.ExternalServiceFactory;
import com.tesora.dve.hazelcast.HazelcastGroupMember;
import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.locking.impl.ClusterLockManager;

public class HazelcastCoordinationServices extends HazelcastGroupMember implements CoordinationServices, GroupMembershipListener {

	public static final String TYPE = "hazelcast";
	private static final int CLUSTER_PORT_DEFAULT = NetworkConfig.DEFAULT_PORT;
	private static final String CLUSTER_PORT_PROPERTY = "cluster.port";
	private static final String GLOBAL_SESS_VAR_MAP_NAME = "DVE.Global.Session.Variables";

	static Logger logger = Logger.getLogger(HazelcastCoordinationServices.class);

	static class Factory implements CoordinationServices.Factory {
		@Override
		public CoordinationServices newInstance() {
			return new HazelcastCoordinationServices();
		}
	}

    ClusterLockManager lockManager;

    SimpleMembershipView currentView = SimpleMembershipView.disabledView();

	String serverIdentity = null;
	InetSocketAddress ourClusterAddress = null;
	HazelcastInstance ourHazelcastInstance = null;

	public HazelcastCoordinationServices() {
	}

	@Override
	public Lock getLock(Object obj) {
		return getOurHazelcastInstance().getLock(obj);
	}

    public ClusterLockManager getLockManager(){
        return lockManager;
    }

    @Override
    public ClusterLock getClusterLock(String name) {
        return lockManager.getClusterLock(name);
    }

	@Override
	protected HazelcastInstance getOurHazelcastInstance() {
		if (ourHazelcastInstance == null) {
			ourHazelcastInstance = Hazelcast.getHazelcastInstanceByName(HAZELCAST_INSTANCE_NAME);
		}
		return ourHazelcastInstance;
	}

	public String getOurIPAddress() {
		return ourClusterAddress.getAddress().getHostAddress() + ":" + ourClusterAddress.getPort();
	}

	@Override
	public <M> GroupTopic<M> getTopic(String name) {
		return new HazelcastGroupTopic<M>(getOurHazelcastInstance(), name);
	}

	@Override
	public InetSocketAddress getMemberAddress() {
		return getOurHazelcastInstance().getCluster().getLocalMember().getInetSocketAddress();
	}

	@Override
	public void addMembershipListener(GroupMembershipListener listener) {
		getOurHazelcastInstance().getCluster().addMembershipListener(new HazelcastMembershipListener(listener));
	}

	@Override
	public void removeMembershipListener(GroupMembershipListener listener) {
		getOurHazelcastInstance().getCluster().removeMembershipListener(new HazelcastMembershipListener(listener));
	}

	@Override
	public void registerWithGroup(Properties props) throws Exception {
		boolean isRegistered = false;

		InetAddress localHost = null;
		try {
			localHost = InetAddress.getLocalHost();
			initClusterAddress(props);
			initServerIdentity(props);
		} catch (Exception e) {
			logger.fatal("Unable to determine server address - aborting", e);
			System.exit(1);
		}

		Connection con = null;
		try {
 			con = getDBConnection(props);
			List<String> registeredServers = findAllRegisteredServers(con);
			if (registeredServers.isEmpty()) {
				isRegistered = registerAsFirstServer(con, serverIdentity, localHost.getCanonicalHostName());
			} else {
				for (String serverAddress: registeredServers) {
					if (serverAddress.equals(serverIdentity)) {
						isRegistered = true;
						logger.debug("Server already registered: " + serverIdentity);
						break;
					}
				}
			}

			if (!isRegistered) {
				registeredServers = findAllRegisteredServers(con);
				isRegistered = registerServer(con, serverIdentity, localHost.getCanonicalHostName());
			}

			if (!isRegistered) {
				logger.fatal("Unable to register host with Group Services");
				System.exit(1);
			}

			startHazelcastServices(registeredServers);

			// register DVE server so load balancer can find it
            // TODO: this duplicates a call to HostService.getPortalPort(), but fixes a circular dependency at construction time. -sgossard
            int portalPort = Integer.parseInt(props.getProperty(
                    PEConstants.MYSQL_PORTAL_PORT_PROPERTY,
                    PEConstants.MYSQL_PORTAL_DEFAULT_PORT));
            InetSocketAddress peServerAddress = new InetSocketAddress(getMemberAddress().getAddress(),
                    portalPort);
			getPEServerAddressMap().put(getMemberAddress(), peServerAddress);
			logger.debug("Registered PE server address with cluster: " + peServerAddress);

			CatalogDAOFactory.setup(props);

			addMembershipListener(this);
			determineQuorumStatus();

            lockManager = new ClusterLockManager(this);
            Singletons.replace(LockManager.class, lockManager);

		} catch (Throwable t) {
			logger.fatal("Unable to register host with Group Services - aborting", t);
			System.exit(1);
		} finally {
			if (con != null)
				con.close();
		}
	}

    private void initClusterAddress(Properties props) throws Exception {
		// TODO when we we fix PE-997 we can defer this to Hazelcast
		InetAddress publicAddress = null;
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		while (interfaces.hasMoreElements() && publicAddress == null) {
			Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
			InetAddress address = null;
			while (addresses.hasMoreElements()) {
				address = addresses.nextElement();
				if (!address.isLoopbackAddress() && address instanceof Inet4Address) {
					publicAddress = address;
					break;
				}
			}
		}
		if (publicAddress == null) {
			throw new PEException("No suitable network interface found for cluster communications.");
		}
		int port = props.containsKey(CLUSTER_PORT_PROPERTY) ? Integer.valueOf(props.getProperty(CLUSTER_PORT_PROPERTY)) : CLUSTER_PORT_DEFAULT;
		ourClusterAddress = new InetSocketAddress(publicAddress, port);
		logger.debug("Cluster address will be: " + ourClusterAddress);
	}

	private void initServerIdentity(Properties props) {
		// TODO For now, server identity == cluster address (see PE-997)
		serverIdentity = ourClusterAddress.getAddress().getHostAddress() + ":" + ourClusterAddress.getPort();
		logger.debug("Server identity: " + serverIdentity);
	}

	private boolean registerServer(Connection con, String address, String name) throws SQLException {
		Statement stmt = con.createStatement();
		stmt.executeUpdate("insert into " + catalog + ".server values (null, '" + address + "', '" + name + "')");
		stmt.close();
		logger.debug("Registered as server: " + address);
		return true;
	}

	private boolean registerAsFirstServer(Connection con, String address, String name) throws SQLException {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("insert into " + catalog + ".server values (1, '" + address + "', '" + name + "')");
			stmt.close();
			logger.debug("Registered as first server in cluster: " + address);
			return true;
		} catch (SQLException e) {
			if (e.getSQLState().startsWith("23")) {
				// integrity constraint violation: another server got in first
				return false;
			}
			throw e;
		}
	}

	private void startHazelcastServices(List<String> registeredServers) throws PEException {
		Config cfg = new Config();

		cfg.setInstanceName(HAZELCAST_INSTANCE_NAME);
		cfg.setProperty("hazelcast.logging.type", "log4j");

		GroupConfig group = cfg.getGroupConfig();
		group.setName(HAZELCAST_GROUP_NAME);
		group.setPassword(HAZELCAST_GROUP_PASSWORD);

		NetworkConfig network = cfg.getNetworkConfig();
		network.setPortAutoIncrement(false);
		network.setPublicAddress(ourClusterAddress.getAddress().getHostAddress());
		network.setPort(ourClusterAddress.getPort());
		Join join = network.getJoin();
		join.getMulticastConfig().setEnabled(false);

		for (String serverAddress : registeredServers) {
			join.getTcpIpConfig().addMember(serverAddress);
			logger.debug("Added member " + serverAddress);
		}
		join.getTcpIpConfig().setEnabled(true);

		MapConfig mc = new MapConfig(GLOBAL_SESS_VAR_MAP_NAME);
		mc.setStorageType(StorageType.HEAP);
		mc.setTimeToLiveSeconds(0);
		mc.setMaxIdleSeconds(0);
		MaxSizeConfig msc = new MaxSizeConfig();
		msc.setSize(0);
		msc.setMaxSizePolicy(MaxSizeConfig.POLICY_CLUSTER_WIDE_MAP_SIZE);
		mc.setMaxSizeConfig(msc);
		
		cfg.addMapConfig(mc);
				
		ourHazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
	}

	private void determineQuorumStatus() throws PEException {
		CatalogDAO catalog = CatalogDAOFactory.newInstance();
		try {
			this.currentView = SimpleMembershipView.buildView(catalog, this);
			if ( ! currentView.isInQuorum() ) {
				logger.warn("PE Server no longer in Quorum - processing disabled");
				if (logger.isDebugEnabled()) {
					for (InetSocketAddress server : currentView.activeQuorumMembers())
						logger.debug("Reachable server: " + server);
					for (InetSocketAddress server : currentView.inactiveQuorumMembers())
						logger.debug("Unreachable server: " + server);
				}
			}
			logger.info("Group Services quorum status: " + currentView.isInQuorum() );
		} finally {
			catalog.close();
		}

	}

	@Override
	public void unRegisterWithGroup() {
		try {
			removeMembershipListener(this);
		} catch (IllegalStateException e) {
			// Hazelcast has already shut down, so we're good
		}

		removeServerRecord(getOurIPAddress());

        lockManager.shutdown();
//		CatalogDAO catalog = CatalogDAOFactory.newInstance();
//		try {
//			catalog.begin();
//			ourRegistrationRecord = catalog.findByKey(ServerRegistration.class, ourRegistrationRecord.getId());
//			catalog.remove(ourRegistrationRecord);
//			catalog.commit();
//			getOurHazelcastInstance().getLifecycleService().shutdown();
//		} finally {
//			catalog.close();
//		}
	}

    public MembershipView getMembershipView(){
        return currentView;
    }

	@Override
	public void onMembershipEvent(MembershipEventType eventType,
			InetSocketAddress inetSocketAddress) {
		try {
			logger.debug("Membership event: " + eventType + "/" + inetSocketAddress);
			determineQuorumStatus();
		} catch (PEException excp) {
			logger.warn("Exception encountered processing group membership event - processing disabled", excp);
			currentView = SimpleMembershipView.disabledView();
		} catch (Throwable thr) {
			logger.warn("Exception encountered processing group membership event - processing disabled", thr);
			currentView = SimpleMembershipView.disabledView();
			throw thr;
		}
		updateExternalServices(eventType, inetSocketAddress);
	}

	@Override
	public void configureProperties(Properties props) {
		props.setProperty("hibernate.cache.use_second_level_cache", "true");
		props.setProperty(GroupManager.HIBERNATE_CACHE_REGION_FACTORY_CLASS,
				HazelcastCacheConfigurator.TYPE);
	}

	public ConcurrentMap<Integer, String> getConnectionMap() {
		return getOurHazelcastInstance().getMap("ConnectionMap");
	}

	@Override
	public int registerConnection(String string) {
		int connectionId = (int) getOurHazelcastInstance().getIdGenerator("ConnectionId").newId();
		getConnectionMap().put(connectionId, string);
		return connectionId;
	}

	@Override
	public void unRegisterConnection(int currentConnId) {
		getConnectionMap().remove(currentConnId);
	}

	@Override
	public boolean localMemberIsOldestMember() {
		Cluster cluster = getOurHazelcastInstance().getCluster();
		return cluster.getLocalMember() == cluster.getMembers().iterator().next();
	}

	static String EXTERNAL_SERVICE_MAP_NAME = "pe_external_services";

	private ConcurrentMap<String, String> getExternalServicesMap() {
		return getOurHazelcastInstance().getMap(EXTERNAL_SERVICE_MAP_NAME);
	}

	@Override
	public String registerExternalService(String name) {
		return getExternalServicesMap().putIfAbsent(name, getOurIPAddress());
	}

	@Override
	public String getExternalServiceRegisteredAddress(String name) {
		return getExternalServicesMap().get(name);
	}

	@Override
	public void deregisterExternalService(String name) {
		if (logger.isDebugEnabled()) {
			logger.debug("Deregistered service '" +name + "' from group.");
		}
		getExternalServicesMap().remove(name);
	}

	private void updateExternalServices(MembershipEventType eventType,
			InetSocketAddress inetSocketAddress) {
		ExternalServiceFactory.onGroupMembershipEvent(eventType, inetSocketAddress);
		// TODO: for now do nothing with event, since we want only have the one external service
		/// and we want the replication slave to be restricted to the starting instance
//		if (eventType == MembershipEventType.MEMBER_REMOVED) {
//			handleExternalServiceMemberRemovedEvent(inetSocketAddress);
//		}
	}

	@Override
	public long getGloballyUniqueId(String domain) {
		return getOurHazelcastInstance().getIdGenerator(domain).newId();
	}

	private Map<String, String> getGlobalVariables() {
		return getOurHazelcastInstance().getMap(GLOBAL_SESS_VAR_MAP_NAME);
	}

//	private void handleExternalServiceMemberRemovedEvent(InetSocketAddress inetSocketAddress) {
//		String memberAddress = inetSocketAddress.getHostName();
//		for(String externalServiceName : getExternalServicesMap().keySet()) {
//			System.out.println("Member removed.  Updating service: " + externalServiceName);
//
//			String externalServiceAddress = getExternalServicesMap().get(externalServiceName);
//
//			System.out.println("Member removed.  External service started on (" + externalServiceAddress + ") and machine leaving is (" + memberAddress + ")");
//			if (StringUtils.equals(memberAddress, externalServiceAddress)) {
//				System.out.println("Member removed.  Site starting the service has stopped communicating.");
//				// force deregistration of existing service
//				deregisterExternalService(externalServiceName);
//
//				if (isInQuorum()) {
//					System.out.println("Member removed.  In quorum starting external services.");
//					startExternalServices();
//				}
//			}
//		}
//	}
//
//	private void startExternalServices() {
//		// start all services and only one should be started
//		CatalogDAO c = CatalogDAOFactory.newInstance();
//		try {
//			// Create and configure External Services
//			for (ExternalService es : c.findAllExternalServices()) {
//				// enclose in try/catch in case an early service fails we still
//				// start the later ones
//				try {
//					ExternalServiceFactory.register(es.getName(), es.getPlugin());
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//		} finally {
//			if (c != null) {
//				c.close();
//			}
//		}
//	}

	@Override
	public String getGlobalVariable(String name) {
		return getGlobalVariables().get(name);
	}

	@Override
	public void setGlobalVariable(String name, String value) {
		getGlobalVariables().put(name, value);
	}
}
