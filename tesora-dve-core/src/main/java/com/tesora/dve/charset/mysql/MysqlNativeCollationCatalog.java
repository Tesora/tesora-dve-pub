// OS_STATUS: public
package com.tesora.dve.charset.mysql;

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

import java.util.List;

import com.tesora.dve.charset.NativeCollationCatalog;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.Collations;
import com.tesora.dve.exceptions.PEException;

public class MysqlNativeCollationCatalog extends NativeCollationCatalog {

	private static final long serialVersionUID = 1L;
	
	public static MysqlNativeCollationCatalog DEFAULT_CATALOG = new MysqlNativeCollationCatalog() {
		private static final long serialVersionUID = 1L;
		{
			for(MysqlNativeCollation collation : MysqlNativeCollation.supportedCollations) {
				addCollation(collation);
			}
		}
	};

	@Override
	public void load() throws PEException {
		if ( !CatalogDAOFactory.isSetup() )
			throw new PEException("Cannot load collations from the catalog as the catalog is not setup.");
		
		CatalogDAO catalog = CatalogDAOFactory.newInstance();
		try {
			List<Collations> collations = catalog.findAllCollations();
			if (collations.size() > 0) {
				for(Collations collation : collations) {
					addCollation(new MysqlNativeCollation(collation.getName(),
							collation.getCharacterSetName(),
							collation.getId(),
							collation.getIsDefault(),
							collation.getIsCompiled(), 
							collation.getSortlen()));
				}
			}
		} finally {
			catalog.close();
		}
	}

}
