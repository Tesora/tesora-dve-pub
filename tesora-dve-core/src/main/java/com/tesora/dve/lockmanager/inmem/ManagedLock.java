// OS_STATUS: public
package com.tesora.dve.lockmanager.inmem;

import com.tesora.dve.lockmanager.AcquiredLock;
import com.tesora.dve.lockmanager.LockClient;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.lockmanager.LockType;

public class ManagedLock implements AcquiredLock {

	private LockSpecification target;
	private LockType type;
	private Thread caller;
	private LockClient client;
	
	private boolean acquired;
	private String reason;
	
	public ManagedLock(LockSpecification target, Thread caller, LockType type, LockClient c) {
		this.target = target;
		this.type = type;
		this.caller = caller;
		this.client = c;
		this.acquired = false;
		this.reason = null;
	}

	@Override	
	public LockSpecification getTarget() { return target; }
	@Override
	public LockType getType() { return type; }
	public Thread getCaller() { return caller; }
	@Override
	public LockClient getClient() { return client; }

	synchronized void setAcquired(String cause) {
		this.acquired = true;
		this.reason = cause;
		if (reason == null)
			throw new IllegalArgumentException("Must specify cause for acquisition");
		this.notify();
	}
	
	// put some state in this thing
	@Override
	public synchronized boolean acquiredLock() {
		return this.acquired;
	}
	
	@Override
	public synchronized String getReason() {
		return reason;
	}
	
	@Override
	public String toString() {
		return target.getName() + "@" + client.getName() + ": " + type + ";acquired=" + acquired + " (" + target.getOriginator() + ")";
	}
}
