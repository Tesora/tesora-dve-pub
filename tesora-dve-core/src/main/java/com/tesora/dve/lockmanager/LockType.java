// OS_STATUS: public
package com.tesora.dve.lockmanager;

public enum LockType {

	// exclusive lock request - no reads or writes
	EXCLUSIVE(3),
	// read shared - selects
	RSHARED(1),
	// write shared - inserts, updates, insert into select, deletes
	WSHARED(2);

	private final int rank;
	
	private LockType(int r) {
		rank = r;
	}

	// return true if this rank is strictly stronger than the other one.
	public boolean isStrongerThan(LockType o) {
		return rank > o.rank;
	}	
}
