// OS_STATUS: public
package com.tesora.dve.sql.statement;

import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.util.ListSet;

public interface CacheableStatement {

	
	public LockType getLockType();
	
	public ListSet<TableKey> getAllTableKeys();

}
