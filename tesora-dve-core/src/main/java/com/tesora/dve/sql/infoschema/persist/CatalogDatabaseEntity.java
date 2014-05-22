package com.tesora.dve.sql.infoschema.persist;

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

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.SimplePersistedEntity;

public class CatalogDatabaseEntity extends SimplePersistedEntity {

	public CatalogDatabaseEntity(CatalogSchema schema, String name, int groupid, String charSet, String collation) throws PEException {
		super(schema.getDatabase());
		preValue("default_group_id",groupid);
		preValue("default_character_set_name",charSet);
		preValue("default_collation_name",collation);
		preValue("name",name);
		preValue("multitenant_mode",MultitenantMode.OFF.getPersistentValue());
		preValue("fk_mode",FKMode.STRICT.getPersistentValue());
	}
}
