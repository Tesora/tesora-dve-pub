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

import com.tesora.dve.charset.NativeCollation;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.SimplePersistedEntity;

public class CatalogCollationEntity extends SimplePersistedEntity {

	public CatalogCollationEntity(final CatalogSchema s, final NativeCollation collation) throws PEException {
		super(s.getCollations());
		preValue("name", collation.getName());
		preValue("character_set_name", collation.getCharacterSetName());
		preValue("id", collation.getId());
		preValue("is_default", collation.isDefault());
		preValue("is_compiled", collation.isCompiled());
		preValue("sortlen", collation.getSortLen());
	}
}
