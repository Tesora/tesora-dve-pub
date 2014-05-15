// OS_STATUS: public
package com.tesora.dve.lockmanager;

import java.util.LinkedHashMap;

import com.tesora.dve.singleton.Singletons;

// we now acquire table locks (read or exclusive) during parsing, which means we may end up
// acquiring the lock first as exclusive and then as shared - but the cluster lock manager will consider
// the second lock to be blocked.  This class arranges to avoid that.
public class AcquiredLockSet {

	private final LinkedHashMap<LockSpecification,AcquiredLock> acquired;
	
	public AcquiredLockSet() {
		acquired = new LinkedHashMap<LockSpecification,AcquiredLock>();
	}
	
	public void acquire(LockClient conn, LockSpecification ls, LockType lt) {
		AcquiredLock already = acquired.get(ls);
		if (already != null) {
			if (lt.isStrongerThan(already.getType())) {
				getLockManager().release(already);
				already = getLockManager().acquire(conn, ls, lt);
				acquired.put(ls,already);
			}
			return;
		}
		already = getLockManager().acquire(conn, ls, lt);
		acquired.put(ls, already);
	}

    public void release() {
		for(AcquiredLock al : acquired.values())
			getLockManager().release(al);
		acquired.clear();
	}

    protected LockManager getLockManager() {
        return Singletons.lookup(LockManager.class);
    }

}
