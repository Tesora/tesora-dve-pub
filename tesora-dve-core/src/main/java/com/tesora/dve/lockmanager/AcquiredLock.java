// OS_STATUS: public
package com.tesora.dve.lockmanager;

public interface AcquiredLock {
    LockSpecification getTarget();
	public LockType getType();
	public LockClient getClient();
	boolean acquiredLock();
	String getReason();
}
