// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PathBasedLock;
import com.tesora.dve.sql.schema.SchemaContext;

public class ShapeLock extends PathBasedLock {

	public ShapeLock(String reason, SchemaContext sc, String localName, PETable tab) {
		this(reason, tab.getDatabase(sc).getName().getUnquotedName().get(),
				localName,
				tab.getTypeHash());
	}
	
	public ShapeLock(String reason, String dbName, String shapeName, String typeHash) {
		super(reason,dbName,shapeName,typeHash);
	}

	@Override
	public boolean isTransactional() {
		return false;
	}

}
