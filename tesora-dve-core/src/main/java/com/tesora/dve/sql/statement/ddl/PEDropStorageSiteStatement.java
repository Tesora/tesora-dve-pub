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
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.TranslatorUtils;
import com.tesora.dve.sql.schema.PEStorageSite;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;

public class PEDropStorageSiteStatement extends
		PEDropStatement<PEStorageSite, PersistentSite> {

	public PEDropStorageSiteStatement( 
			Persistable<PEStorageSite, PersistentSite> targ) {
		super(PEStorageSite.class, null, true, targ, TranslatorUtils.PERSISTENT_SITE_TAG);
	}

	public void ensureUnreferenced(SchemaContext pc) {
		PEStorageSite pess = getTarget().get();
		try {
			pc.beginSaveContext();
			PersistentSite ss = null;
			try {
				pess.persistTree(pc,true);
				ss = pess.getPersistent(pc);
			} finally {
				pc.endSaveContext();
			}
			HashMap<String,Object> params = new HashMap<String,Object>();
			params.put("ss",ss);
			String q = "select gen.storageGroup from StorageGroupGeneration gen where :ss member of gen.groupMembers";
			List<CatalogEntity> any = pc.getCatalog().query(q,params);
			
				//.query("select gen.storageGroup from StorageGroupGeneration gen where gen.groupMembers = :ss", params);
			if (!any.isEmpty()) {
				PersistentGroup pg = (PersistentGroup)any.get(0);
				throw new SchemaException(Pass.PLANNER,"Unable to drop persistent site " + pess.getName().get() + " because used by group " + pg.getName());
			}
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to compute reference set for site " + pess.getName().get());
		}
	}
}
