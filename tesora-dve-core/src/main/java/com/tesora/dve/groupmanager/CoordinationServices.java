// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.lockmanager.LockManager;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.locks.Lock;

public interface CoordinationServices extends MembershipViewSource {
	
	interface Factory {
		public CoordinationServices newInstance();
	}
	
	Lock getLock(Object obj);

    LockManager getLockManager();
    ClusterLock getClusterLock(String name);

	<M> GroupTopic<M> getTopic(String name);
	
	void addMembershipListener(GroupMembershipListener listener);
	
	void removeMembershipListener(GroupMembershipListener listener);

	InetSocketAddress getMemberAddress();
	
	void registerWithGroup(Properties props) throws Exception;
	void unRegisterWithGroup();
    MembershipView getMembershipView();

	void shutdown();

	Collection<InetSocketAddress> getMembers();

	void configureProperties(Properties props);

	int registerConnection(String string);

	void unRegisterConnection(int currentConnId);

	boolean localMemberIsOldestMember();

	String registerExternalService(String name);

	void deregisterExternalService(String name);

	String getExternalServiceRegisteredAddress(String name);
	
	long getGloballyUniqueId(String domain);
}
