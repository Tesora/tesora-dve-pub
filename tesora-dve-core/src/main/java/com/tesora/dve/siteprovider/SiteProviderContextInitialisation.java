// OS_STATUS: public
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
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.siteprovider.SiteProviderPlugin.SiteProviderContext;

public class SiteProviderContextInitialisation implements SiteProviderContext {
	
	String providerName;
	CatalogDAO catalogDAO;

	public SiteProviderContextInitialisation(String providerName, CatalogDAO catalogDAO) {
		super();
		this.providerName = providerName;
		this.catalogDAO = catalogDAO;
	}

	@Override
	public void setGlobalVariable(String variableName, String value) throws PEException {
        Singletons.require(HostService.class).setGlobalVariable(getCatalogDAO(), variableName, value);
	}

	@Override
	public String getGlobalVariable(String variableName) throws PEException {
        return Singletons.require(HostService.class).getGlobalVariable(getCatalogDAO(), variableName);
	}

	@Override
	public void setProviderEnabled(final boolean isEnabled) throws PEException {
		try {
			getCatalogDAO().new EntityUpdater() {
				@Override
				public CatalogEntity update() throws Throwable {
					Provider p = catalogDAO.findProvider(providerName);
					p.setIsEnabled(isEnabled);
					return p;
				}
			}.execute();
		} catch (Throwable e) {
			throw new PEException("Unable to update persistent provider " + providerName, e);
		}
	}

	@Override
	public boolean getProviderEnabled() throws PEException {
		return getCatalogDAO().findProvider(providerName).isEnabled();
	}

	@Override
	public void setProviderConfig(final String config) throws PEException {
		try {
			getCatalogDAO().new EntityUpdater() {
				@Override
				public CatalogEntity update() throws Throwable {
					Provider p = catalogDAO.findProvider(providerName);
					p.setConfig(config);
					return p;
				}
			}.execute();
		} catch (Throwable e) {
			throw new PEException("Unable to update persistent provider " + providerName, e);
		}
	}

	@Override
	public String getProviderConfig() throws PEException {
		return getCatalogDAO().findProvider(providerName).getConfig();
	}

	protected CatalogDAO getCatalogDAO() {
		return catalogDAO;
	}
}
