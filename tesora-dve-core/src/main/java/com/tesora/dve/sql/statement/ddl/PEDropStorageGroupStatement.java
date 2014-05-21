// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

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

import java.util.HashMap;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.TranslatorUtils;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;

public class PEDropStorageGroupStatement extends
		PEDropStatement<PEPersistentGroup, PersistentGroup> {
	
	public PEDropStorageGroupStatement( 
			Persistable<PEPersistentGroup, PersistentGroup> targ) {
		super(PEPersistentGroup.class, null, true, targ, TranslatorUtils.PERSISTENT_GROUP_TAG);
	}

	public void ensureUnreferenced(SchemaContext pc) {
		try {
			String any = getReferencingUserData(pc,getTarget().get());
			if (any != null)
				throw new SchemaException(Pass.PLANNER, "Unable to drop persistent group " + getTarget().get().getName() + " because used by " + any);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to compute referenced set of persistent group",pe);
		}
	}
	
	private String getReferencingUserData(SchemaContext sc, PEPersistentGroup pesg) throws PEException {
		sc.beginSaveContext();
		PersistentGroup pg = null;
		try {
			pesg.persistTree(sc,true);
			pg = pesg.getPersistent(sc);
		} finally {
			sc.endSaveContext();
		}
		HashMap<String,Object> params = new HashMap<String, Object>();
		params.put("pg",pg);
		// find any database that references the group as a persistent group
		List<CatalogEntity> any = sc.getCatalog().query("from UserDatabase where defaultStorageGroup = :pg", params);
		if (!any.isEmpty())
			return "database " + ((UserDatabase)any.get(0)).getName();
		// now check tables
		any = sc.getCatalog().query("from UserTable where persistentGroup = :pg", params);
		if (!any.isEmpty())
			return "table " + ((UserTable)any.get(0)).getName();
		// containers
		any = sc.getCatalog().query("from Container where storageGroup = :pg", params);
		if (!any.isEmpty())
			return "container " + ((Container)any.get(0)).getName();
		return null;
	}
	
}
