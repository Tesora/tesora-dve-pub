package com.tesora.dve.sql.schema;

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

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;

public interface PEStorageGroup {

	StorageGroup getPersistent(SchemaContext sc, ConnectionValues cv);

	PersistentGroup persistTree(SchemaContext sc) throws PEException;

	PEPersistentGroup anySite(SchemaContext sc) throws PEException;

	boolean comparable(SchemaContext sc, PEStorageGroup storage);
	
	boolean isSubsetOf(SchemaContext sc, PEStorageGroup storage);

	boolean isSingleSiteGroup();

	void setCost(int score) throws PEException;
	
	boolean isTempGroup();	
	
	PEStorageGroup getPEStorageGroup(SchemaContext sc, ConnectionValues cv);
	
	StorageGroup getScheduledGroup(SchemaContext sc, ConnectionValues cv);
	
}
