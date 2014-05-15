// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.BasicType;

public class CharacterSetsInformationSchemaTable extends
		LogicalInformationSchemaTable {
	
	public CharacterSetsInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName("character_sets"));
		addExplicitColumn("CHARACTER_SET_NAME", buildStringType(dbn, 32));
		addExplicitColumn("DESCRIPTION", buildStringType(dbn,60));
		addExplicitColumn("MAX_LEN", BasicType.buildType(java.sql.Types.BIGINT, 3, dbn));
	}
}
