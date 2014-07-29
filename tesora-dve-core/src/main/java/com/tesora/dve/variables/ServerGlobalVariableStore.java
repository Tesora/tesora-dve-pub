package com.tesora.dve.variables;

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

import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.groupmanager.GroupTopicPublisher;
import com.tesora.dve.groupmanager.OnGlobalConfigChangeMessage;
import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.lockmanager.LockClient;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

/*
 * The global variable store holds both global values as well as session values that have no
 * global equivalent.  The global values come from the group services; the session values come
 * from the catalog.
 */
public class ServerGlobalVariableStore extends AbstractVariableStore implements GlobalVariableStore, LockClient {

	public static final ServerGlobalVariableStore INSTANCE = new ServerGlobalVariableStore();
	
	private static final String GLOBAL_VAR_STORE_LOCK_NAME = "DVE.Global.Variables";
	
	// this is a cache that's built from the group services copy
	// we populate it lazily
	private final ConcurrentHashMap<VariableHandler<?>,ValueReference<?>> cache = 
			new ConcurrentHashMap<VariableHandler<?>,ValueReference<?>>();
	
	// make it public for the trans exec engine
	private ServerGlobalVariableStore() {
		super();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <Type> ValueReference<Type> getReference(VariableHandler<Type> vh) {
		ValueReference vr = cache.get(vh);
		if (vr == null)
			vr = readValue(vh);
		return vr;
	}

	private <Type> ValueReference<Type> readValue(VariableHandler<Type> vh) {
		ClusterLock lock = getLock();
		try {
			lock.sharedLock(this,"repopulate global var cache for read");

			String value = GroupManager.getCoordinationServices().getGlobalVariable(vh.getName());
			if (value == null)
				return null;
			ValueReference<Type> out = new ValueReference(vh,vh.toInternal(value)); 
			cache.put(vh,out);
			return out;
		} catch (PEException pe) {
			throw new VariableException(pe);
		} finally {
			lock.sharedUnlock(this, "repopulate global var cache for read");			
		}
	}
	
	@Override
	public <Type> void setValue(VariableHandler<Type> vh, Type t) {
		// in the write scenario, we lock first, then write through to the group manager map
		String newValue = vh.toMap(t);
		ClusterLock lock = getLock();
		try {
			lock.exclusiveLock(this, "set global var");
			// call into the group manager for this bit
			GroupManager.getCoordinationServices().setGlobalVariable(vh.getName(), newValue);
			// then send a message invalidating
	        Singletons.require(GroupTopicPublisher.class).publish(new OnGlobalConfigChangeMessage(vh.getName(), newValue));
		} finally {
			lock.exclusiveUnlock(this, "set global var");
		}
	}
	
	private ClusterLock getLock() {
		return GroupManager.getCoordinationServices().getClusterLock(GLOBAL_VAR_STORE_LOCK_NAME);
	}
	
	@Override
	public String getName() {
		return "GlobalVariableManager";
	}

	@Override
	public void invalidate(VariableHandler vh) {
		cache.remove(vh);
	}
	
	public LocalVariableStore buildNewLocalStore() {
		VariableManager vm = Singletons.require(HostService.class).getVariableManager();
		LocalVariableStore out = new LocalVariableStore();
		for(VariableHandler vh : vm.getAllHandlers()) {
			out.setInternal(vh, getReference(vh).get());
		}
		return out;
	}

	@Override
	public boolean isServer() {
		return true;
	}
	
}
