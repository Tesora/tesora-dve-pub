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

import java.nio.charset.Charset;
import java.util.List;

import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CharacterSets;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.exceptions.PEException;

public class MysqlNativeCharSetCatalog extends NativeCharSetCatalog {

	private static final long serialVersionUID = 1L;
	
	public static MysqlNativeCharSetCatalog DEFAULT_CATALOG = new MysqlNativeCharSetCatalog() {
		private static final long serialVersionUID = 1L;
		{
			addCharSet(MysqlNativeCharSet.ASCII);
			addCharSet(MysqlNativeCharSet.LATIN1);
			addCharSet(MysqlNativeCharSet.UTF8);
			addCharSet(MysqlNativeCharSet.UTF8MB4);
		}
	};

	@Override
	public void load() throws PEException {
		if ( !CatalogDAOFactory.isSetup() )
			throw new PEException("Cannot load character sets from the catalog as the catalog is not setup.");
		
		CatalogDAO catalog = CatalogDAOFactory.newInstance();
		try {
			List<CharacterSets> characterSets = catalog.findAllCharacterSets();
			if (characterSets.size() > 0) {
				for(CharacterSets cs : characterSets) {
					addCharSet(new MysqlNativeCharSet(cs.getId(), cs.getCharacterSetName(), cs.getDescription(), cs.getMaxlen(), Charset.forName(cs.getPeCharacterSetName())));
				}
			}
		} finally {
			catalog.close();
		}
	}

}
