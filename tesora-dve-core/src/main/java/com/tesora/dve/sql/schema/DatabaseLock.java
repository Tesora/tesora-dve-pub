// OS_STATUS: public
package com.tesora.dve.sql.schema;

public class DatabaseLock extends PathBasedLock {

	public DatabaseLock(String reason, PEDatabase pdb) {
		super(reason, pdb.getName().getUnquotedName().get());
	}
	
	@Override
	public boolean isTransactional() {
		return false;
	}


}
