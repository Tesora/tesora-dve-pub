// OS_STATUS: public
package com.tesora.dve.charset.standalone;

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

import com.tesora.dve.charset.mysql.MysqlNativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSetCatalog;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;

public class StandaloneMysqlNativeCharSetCatalog extends MysqlNativeCharSetCatalog {

	private static final long serialVersionUID = 1L;

	public StandaloneMysqlNativeCharSetCatalog() {
		super();
	}
	
	@Override
	public void load() throws PEException {
		for (MysqlNativeCharSet mncs : MysqlNativeCharSet.supportedCharSets) {
			addCharSet(mncs);
		}
	}

	@Override
	public void save(CatalogDAO c) {
		// do nothing as this implementation does not use the catalog
	}

	
}
