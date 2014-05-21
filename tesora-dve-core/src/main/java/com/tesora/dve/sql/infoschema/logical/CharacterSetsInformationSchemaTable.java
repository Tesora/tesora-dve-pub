package com.tesora.dve.sql.infoschema.logical;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
