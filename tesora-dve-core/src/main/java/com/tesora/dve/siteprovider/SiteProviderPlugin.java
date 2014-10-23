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

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.variables.ScopedVariables;
import com.tesora.dve.worker.SiteManagerCommand;

public interface SiteProviderPlugin {
	
	Logger logger = Logger.getLogger(SiteProviderPlugin.class);

	public interface SiteProviderContext {
		public void setGlobalVariable(String variableName, String value) throws PEException;
		public String getGlobalVariable(String variableName) throws PEException;
		
		// These methods get and set the persistent version of "enabled" in the provider CatalogEntity
		public void setProviderEnabled(boolean isEnabled) throws PEException;
		public boolean getProviderEnabled() throws PEException;
		
		// These methods get and set the persistent provider configuration in the provider CatalogEntity
		public void setProviderConfig(String config) throws PEException;
		public String getProviderConfig() throws PEException;
	}

	public static class SiteProviderFactory {

		static Map<String, SiteProviderPlugin> providerMap = new HashMap<String, SiteProviderPlugin>();

		public static SiteProviderPlugin getInstance(String type) throws PEException {
			return providerMap.get(type);
		}

		public static SiteProviderPlugin getInstance(SiteProviderContext ctxt, String type, String providerClass) throws PEException {
			SiteProviderPlugin spp = providerMap.get(type);

			if (spp == null) {
				if (providerClass == null)
					throw new PEException("No SiteProvider configured for provider type " + type);
				spp = addProvider(ctxt, providerClass, type, true, null);
			}
			return spp;
		}

		public static void deregister(String name) {
			providerMap.remove(name);
		}

		public static SiteProviderPlugin addProvider(SiteProviderContext ctxt, String providerClass, String providerName, boolean isEnabled,
				String config) throws PEException {
			SiteProviderPlugin spp = null;
			try {
				Class<?> siteManagerClass = Class.forName(providerClass);
				Constructor<?> ctor = siteManagerClass.getConstructor(new Class[] {});
				spp = (SiteProviderPlugin) ctor.newInstance(new Object[] {});
				try {
					spp.initialize(ctxt, providerName, isEnabled, config);
				} catch (PEException e) {
					logger.warn("Unable to initialise site provider \"" + providerName + "\" - provider will be disabled", e);
					ctxt.setProviderEnabled(false);
					spp.setEnabled(false);
				}

				providerMap.put(providerName, spp);

                Singletons.require(HostService.class).addScopedConfig(providerName, spp.getVariableConfiguration());
			} catch (Exception e) {
				throw new PEException("Cannot instantiate SiteProvider '" + providerClass + "'", e);
			}
			return spp;
		}

		public static void closeSiteProviders() {
			for (SiteProviderPlugin spp : providerMap.values()) {
				spp.close();
			}
			providerMap.clear();
		}
	}

	void initialize(SiteProviderContext ctxt, String name, boolean isEnabled, String config) throws PEException;

	ScopedVariables getVariableConfiguration() throws PEException;

	boolean isEnabled();
	void setEnabled(boolean isEnabled) throws PEException;

	String getProviderName();

	void close();

	void returnSitesByClass(SiteProviderContext ctxt, String siteClass, Collection<? extends StorageSite> sites) throws PEException;

	void provisionWorkerRequest(SiteProviderContext ctxt, GetWorkerRequest getWorkerRequest) throws PEException;

	// for show commands - return what will be shown
	List<CatalogEntity> show(SiteManagerCommand smc) throws PEException;

	// for updates - coordinate with catalog updates
	// prepare the update
	SiteManagerCommand prepareUpdate(SiteManagerCommand smc) throws PEException;

	// actually do the update - the object passed in is the one returned from
	// prepare return the rowcount of the update, usually 1 but could be more I
	// suppose
	int update(SiteManagerCommand smc) throws PEException;

	// if the update was unsuccessful, rollback will be called - after the
	// catalog rollback the object passed in is the one returned from prepare
	void rollback(SiteManagerCommand smc) throws PEException;
	
	// for show site status - need to get the current set of dynamic sites
	Collection<? extends StorageSite> getAllSites();
}
