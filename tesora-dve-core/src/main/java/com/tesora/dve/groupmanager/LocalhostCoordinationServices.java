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

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tesora.dve.lockmanager.LockManager;
import com.tesora.dve.locking.impl.CoordinationServices;
import com.tesora.dve.membership.GroupMembershipListener;
import com.tesora.dve.membership.GroupTopic;
import com.tesora.dve.membership.MembershipView;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.membership.GroupMembershipListener.MembershipEventType;
import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.locking.impl.ClusterLockManager;

public class LocalhostCoordinationServices implements CoordinationServices {
	
	static Logger logger = Logger.getLogger(LocalhostCoordinationServices.class);

	static class Factory implements CoordinationServices.Factory {
		@Override
		public CoordinationServices newInstance() {
			return new LocalhostCoordinationServices();
		}
	}

	public static final String TYPE = "localhost";
	
	private Set<InetSocketAddress> members = Collections.newSetFromMap(new ConcurrentHashMap<InetSocketAddress, Boolean>());
	private AtomicInteger memberId = new AtomicInteger();
	
	private Set<GroupMembershipListener> membershipListeners = Collections.newSetFromMap(new ConcurrentHashMap<GroupMembershipListener, Boolean>());

	private ConcurrentHashMap<String, LocalTopic<?>> topicMap = new ConcurrentHashMap<String, LocalTopic<?>>();

    ClusterLockManager lockManager;
	
	InetSocketAddress thisMember;

	private ConcurrentMap<Integer, String> connectionMap = new ConcurrentHashMap<Integer, String>();

	private AtomicInteger globalConnectionId = new AtomicInteger();
	
	private ConcurrentMap<String, String> externalServicesMap = new ConcurrentHashMap<String, String>();
	
	private final HashMap<String, String> globalVariables = new HashMap<String,String>();

	public LocalhostCoordinationServices() {
//		thisMember = new InetSocketAddress(getClass().getSimpleName(),memberId.incrementAndGet());
		thisMember = InetSocketAddress.createUnresolved(getClass().getSimpleName(),memberId.incrementAndGet());
		if (logger.isInfoEnabled())
			logger.info("Configuring group services " + getClass().getSimpleName() + "(" + thisMember + ")");
		
		members.add(thisMember);

        lockManager = new ClusterLockManager(this);

		for (GroupMembershipListener l : membershipListeners)
			l.onMembershipEvent(MembershipEventType.MEMBER_ADDED, getMemberAddress());
		
		// load the global vars somehow here
	}

	@Override
	public Lock getLock(Object obj) {
		return new ReentrantLock();
	}

    public ClusterLockManager getLockManager(){
        return lockManager;
    }

    @Override
    public ClusterLock getClusterLock(String name) {
        return lockManager.getClusterLock(name);
    }

	@SuppressWarnings("unchecked")
	@Override
	public <M> GroupTopic<M> getTopic(String name) {
		if (false == topicMap.containsKey(name))
			topicMap.putIfAbsent(name, new LocalTopic<M>());
		return (GroupTopic<M>) topicMap.get(name);
	}

	@Override
	public void addMembershipListener(GroupMembershipListener listener) {
		membershipListeners.add(listener);
	}

	@Override
	public void removeMembershipListener(GroupMembershipListener listener) {
		membershipListeners.remove(listener);
	}

	@Override
	public void shutdown() {
		globalVariables.clear();
		members.remove(getMemberAddress());
		for (GroupMembershipListener l : membershipListeners)
			l.onMembershipEvent(MembershipEventType.MEMBER_REMOVED, getMemberAddress());
		
        //TODO: how should we handle this for distributed locks? -sgossard
		//rwLockMap.clear();
		topicMap.clear();
	}

	@Override
	public InetSocketAddress getMemberAddress() {
		return thisMember;
	}

	@Override
	public Collection<InetSocketAddress> getMembers() {
		return members;
	}
	
	public void reset() {
		members.clear();
		membershipListeners.clear();
		topicMap.clear();
	}

	@Override
	public void registerWithGroup(Properties props) {
        Singletons.replace(LockManager.class, lockManager);
	}

	@Override
	public void unRegisterWithGroup() {
	}

    @Override
    public MembershipView getMembershipView(){
        return SimpleMembershipView.buildView(this.getMemberAddress(), this.getMembers(), this.getMembers());
    }

	@Override
	public void configureProperties(Properties props) {
//		if (!props.containsKey(GroupManager.HIBERNATE_CACHE_REGION_FACTORY_CLASS)) {
//			props.setProperty("hibernate.cache.use_second_level_cache", "true");
//			props.setProperty(GroupManager.HIBERNATE_CACHE_REGION_FACTORY_CLASS, 
//					EHCacheCacheConfigurator.TYPE1);
//		}
	}

	public ConcurrentMap<Integer, String> getConnectionMap() {
		return connectionMap;
	}

	@Override
	public int registerConnection(String string) {
		int connectionId = globalConnectionId.incrementAndGet();
		connectionMap.put(connectionId, string);
		return connectionId;
	}
	
	@Override
	public void unRegisterConnection(int currentConnId) {
		getConnectionMap().remove(currentConnId);
	}

	@Override
	public boolean localMemberIsOldestMember() {
		return true;
	}

	public ConcurrentMap<String, String> getExternalServicesMap() {
		return externalServicesMap;
	}
	
	@Override
	public String registerExternalService(String name) {
		return getExternalServicesMap().putIfAbsent(name, getMemberAddress().getHostName());
	}
	
	@Override
	public String getExternalServiceRegisteredAddress(String name) {
		return getExternalServicesMap().get(name);
	}
	
	@Override
	public void deregisterExternalService(String name) {
		getExternalServicesMap().remove(name);
	}
	
	static AtomicLong globalIdGenerator = new AtomicLong();

	@Override
	public long getGloballyUniqueId(String domain) {
		return globalIdGenerator.incrementAndGet();
	}

	@Override
	public synchronized String getGlobalVariable(String name) {
		return globalVariables.get(name);
	}

	@Override
	public synchronized void setGlobalVariable(String name, String value) {
		globalVariables.put(name, value);
	}

}
