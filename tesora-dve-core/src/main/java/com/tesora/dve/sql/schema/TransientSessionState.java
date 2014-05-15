// OS_STATUS: public
package com.tesora.dve.sql.schema;

public class TransientSessionState {

	private boolean underLockTable = false;
	
	public TransientSessionState() {
		
	}
	
	public void setSawLockTable() {
		underLockTable = true;
	}
	
	public void setSawUnlockTable() {
		underLockTable = false;
	}
	
	public boolean isUnderLockTable() {
		return underLockTable;
	}
	
}
