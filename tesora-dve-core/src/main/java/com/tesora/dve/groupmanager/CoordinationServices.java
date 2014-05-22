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
