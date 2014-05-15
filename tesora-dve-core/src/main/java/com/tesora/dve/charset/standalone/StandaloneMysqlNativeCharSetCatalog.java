// OS_STATUS: public
package com.tesora.dve.charset.standalone;

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
