// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.lockmanager.LockType;

public class LockInfo {

	private final String reason;
	private final LockType type;
	
	public LockInfo(LockType type, String reason) {
		this.reason = reason;
		this.type = type;
	}

	public String getReason() {
		return reason;
	}
	
	public LockType getType() {
		return type;
	}
	
}
