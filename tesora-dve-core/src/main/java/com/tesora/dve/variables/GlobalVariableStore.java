package com.tesora.dve.variables;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.lockmanager.LockClient;

/*
 * The global variable store holds both global values as well as session values that have no
 * global equivalent.  The global values come from the group services; the session values come
 * from the catalog.
 */
public class GlobalVariableStore extends VariableStore implements LockClient {

	public static final GlobalVariableStore INSTANCE = new GlobalVariableStore();
	
	private static final String GLOBAL_VAR_STORE_LOCK_NAME = "DVE.Global.Variables";
	
	// this is a cache that's built from the group services copy
	// we populate it lazily
	private ConcurrentHashMap<String,ValueReference<?>> cache;
	
	private ConcurrentHashMap<String,ValueReference<?>> sessionOnlyDefaults;
	
	public GlobalVariableStore() {
		super();
		cache = null;
		sessionOnlyDefaults = null;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <Type> ValueReference<Type> getReference(VariableHandler<Type> vh) {
		return (ValueReference<Type>) read().get(vh.getName());
	}

	private ConcurrentHashMap<String,ValueReference<?>> read() {
		ConcurrentHashMap<String, ValueReference<?>> out = cache;
		if (out == null) {
			ClusterLock lock = getLock();
			lock.sharedLock(this,"repopulate global var cache for read");
			// repopulate from the group manager
			out = new ConcurrentHashMap<String,ValueReference<?>>();
			lock.sharedUnlock(this, "repopulate global var cache for read");
			if (cache == null)
				cache = out;
		}
		return out;
	}
	
	private ConcurrentHashMap<String,ValueReference<?>> repopulate() throws PEException {
		ConcurrentHashMap<String,ValueReference<?>> out = new ConcurrentHashMap<String,ValueReference<?>>();
		// get a new catalog object, load with that
		Map<String,String> globals = GroupManager.getCoordinationServices().getGlobalVariables();
		HashSet<String> keys = new HashSet<String>(globals.keySet());
		for(VariableHandler<?> vh : Variables.getGlobalHandlers()) {
			if (!keys.remove(vh.getName())) {
				// missing - this is an error
				throw new PECodingException("Missing global value for " + vh.getName());
			}
			String value = globals.get(vh.getName());
			Object converted = vh.getMetadata().convertToInternal(value);
			ValueReference<?> vr = vh.getDefaultValueReference();
			vr.setInternal(converted);
			out.put(vh.getName(), vr);
		}
		if (!keys.isEmpty()) 
			throw new PECodingException("More global variables found that configured");
		ConcurrentHashMap<String,ValueReference<?>> sessVals = sessionOnlyDefaults;
		if (sessVals == null) 
			sessVals = buildSessionOnlyDefaults();
		for(ValueReference<?> vh : sessVals.values()) {
			out.put(vh.getVariable().getName(), vh.copy());
		}
		return out;
	}
	
	@Override
	public <Type> void setValue(VariableHandler<Type> vh, Type t) {
		// in the write scenario, we lock first, then write through to the group manager map
		ClusterLock lock = getLock();
		try {
			lock.exclusiveLock(this, "set global var");
			// call into the group manager for this bit
			
			// then send a message invalidating
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

	public void invalidate() {
		cache = null;
	}
	
	public LocalVariableStore buildNewLocalStore() {
		ConcurrentHashMap<String,ValueReference<?>> copy = read();
		LocalVariableStore out = new LocalVariableStore();
		for(ValueReference<?> vr : copy.values()) {
			VariableHandler<?> vh = vr.getVariable();
			if (vh.getScopes().contains(VariableScope.SESSION)) {
				out.setInternal(vh,vr.get());
			}
		}
		return out;
	}
	
	private ConcurrentHashMap<String,ValueReference<?>> buildSessionOnlyDefaults() {
		// todo: pull value out of catalog, rather than handler
		ConcurrentHashMap<String,ValueReference<?>> out = new ConcurrentHashMap<String,ValueReference<?>>();
		for(VariableHandler<?> vh : Variables.getSessionHandlers()) {
			if (vh.getScopes().contains(VariableScope.GLOBAL)) continue;
			ValueReference<?> vr = vh.getDefaultValueReference();
			// should reset it from the catalog
			out.put(vh.getName(),vr);
		}
		sessionOnlyDefaults = out;
		return out;
	}
}
