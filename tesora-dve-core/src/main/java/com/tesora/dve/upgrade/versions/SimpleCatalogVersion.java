// OS_STATUS: public
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

public abstract class SimpleCatalogVersion extends BasicCatalogVersion {
	
	public SimpleCatalogVersion(int v) {
		this(v,false);
	}

	public SimpleCatalogVersion(int v, boolean infoSchemaUpgrade) {
		super(v, infoSchemaUpgrade);
	}
	
	@Override
	public void upgrade(DBHelper helper) throws PEException {
		for(String c : getUpgradeCommands(helper)) try {
			helper.executeQuery(c);
		} catch (SQLException sqle) {
			throw new PEException("Error executing '" + c + "'",sqle);
		}
		dropVariables(helper, getObsoleteVariables());
	}

	public abstract String[] getUpgradeCommands(DBHelper helper) throws PEException;

	// override this for your variables
	public Collection<String> getObsoleteVariables() {
		return Collections.emptySet();
	}
}
