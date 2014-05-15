// OS_STATUS: public
package com.tesora.dve.siteprovider;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;

public class SiteProviderContextForProvision extends SiteProviderContextInitialisation {
	
	public SiteProviderContextForProvision(String providerName) {
		super(providerName, null);
	}

	public void close() {
		if (catalogDAO != null)
			catalogDAO.close();
	}
	
	@Override
	protected CatalogDAO getCatalogDAO() {
		if (catalogDAO == null)
			catalogDAO = CatalogDAOFactory.newInstance();
		return catalogDAO;
	}

}
