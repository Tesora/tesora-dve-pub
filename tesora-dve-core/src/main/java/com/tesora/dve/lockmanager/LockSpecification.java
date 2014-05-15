// OS_STATUS: public
package com.tesora.dve.lockmanager;

// a named lock
public interface LockSpecification {

    public String getName();

	public String getOriginator();

    // whether this lock respects transactions.  for instance,
	// locks around ddl do not (i.e. we don't want them held past
	// the end of the current statement).
	public boolean isTransactional();	
}
