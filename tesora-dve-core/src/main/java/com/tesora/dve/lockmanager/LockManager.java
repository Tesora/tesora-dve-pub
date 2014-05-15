// OS_STATUS: public
package com.tesora.dve.lockmanager;

import com.tesora.dve.resultset.IntermediateResultSet;

// basic lock manager interface
public interface LockManager {

	// blocks until the lock is acquired or a lock exception is thrown
	// lock type is either shared or exclusive
	// intxn if the caller is in a txn
	AcquiredLock acquire(LockClient c, LockSpecification l, LockType type);
	
	// release the specified lock
	void release(AcquiredLock al);

	public IntermediateResultSet showState();
	
	public String assertNoLocks();	
}
