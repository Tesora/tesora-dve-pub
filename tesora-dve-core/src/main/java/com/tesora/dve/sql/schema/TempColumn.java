// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.sql.schema.types.Type;

public class TempColumn extends PEColumn {

	public TempColumn(SchemaContext pc, Name name, Type type) {
		super(pc, name, type);
	}

	public boolean isTempColumn() {
		return true;
	}
	
}
