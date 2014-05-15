// OS_STATUS: public
package com.tesora.dve.charset.mysql;

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
