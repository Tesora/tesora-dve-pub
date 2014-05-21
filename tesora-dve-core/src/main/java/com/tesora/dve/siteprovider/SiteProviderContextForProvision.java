package com.tesora.dve.siteprovider;

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
