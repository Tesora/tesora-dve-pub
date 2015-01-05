package com.tesora.dve.upgrade.versions;

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

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.InfoSchemaUpgrader;

public abstract class BasicCatalogVersion implements CatalogVersion {

	protected final boolean infoSchemaUpgrade;
	protected final int version;

	public BasicCatalogVersion(int version, boolean infoSchemaUpgrade) {
		this.infoSchemaUpgrade = infoSchemaUpgrade;
		this.version = version;
	}

	@Override
	public boolean hasInfoSchemaUpgrade() {
		return infoSchemaUpgrade;
	}

	@Override
	public int getSchemaVersion() {
		return this.version;
	}
	
	protected void clearInfoSchema(DBHelper helper) throws PEException {
		int gid = InfoSchemaUpgrader.getInfoSchemaGroupID(helper);
		InfoSchemaUpgrader.clearCurrentInfoSchema(helper, gid);
	}
	
	protected void dropVariables(DBHelper helper, Collection<String> varNames) throws PEException {
		if (varNames == null || varNames.isEmpty()) return;
		try {
			helper.prepare("delete from config where name = ?");
			for(String s : varNames) {
				helper.executePrepared(Collections.singletonList((Object)s));
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to drop variables",sqle);
		}
	}
}