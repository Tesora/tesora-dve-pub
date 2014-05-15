// OS_STATUS: public
package com.tesora.dve.sql.schema;


public class TableUseLock extends PathBasedLock {

	
	public TableUseLock(String why, String...vals) {
		super(why, vals);
	}

	@Override
	public boolean isTransactional() {
		return true;
	}

}
