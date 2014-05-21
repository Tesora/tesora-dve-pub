package com.tesora.dve.testprovider;

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

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.siteprovider.SiteProviderContextForProvision;
import com.tesora.dve.siteprovider.SiteProviderPlugin;
import com.tesora.dve.variable.ScopedVariableHandler;
import com.tesora.dve.variable.VariableConfig;
import com.tesora.dve.variable.VariableInfo;
import com.tesora.dve.worker.SiteManagerCommand;

public class StaticSiteProvider implements SiteProviderPlugin {
	
	SiteProviderConfig providerConfig;
	
	String providerName;
	String configString;
	boolean isEnabled;
	
	public StaticSiteProvider() throws PEException {
	}

	private SiteProviderConfig parseXML() throws PEException {
		String configFileName = getProviderName() + ".xml";
		SiteProviderConfig providerConfig;
		try {
			InputStream is = getClass().getResourceAsStream(configFileName);
			if (is == null)
				throw new PEException("Cannot load " + configFileName + " from classpath");
			JAXBContext jc = JAXBContext.newInstance(SiteProviderConfig.class);
			Unmarshaller u = jc.createUnmarshaller();
			providerConfig = (SiteProviderConfig) u.unmarshal(is);
			if (providerConfig.siteClassMap.isEmpty())
				throw new PEException(configFileName + " contained no SiteClasses");
		} catch (JAXBException e) {
			throw new PEException("JAXB exception reading " + configFileName, e);
		}
		return providerConfig;
	}

	@Override
	public void close() {
	}


	@Override
	public void initialize(SiteProviderContext ctxt, String name,
			boolean isEnabled, String config) throws PEException {
		this.providerName = name;
		this.isEnabled = isEnabled;
		this.configString = config;
		providerConfig = parseXML();
	}

	@Override
	public VariableConfig<ScopedVariableHandler> getVariableConfiguration() throws PEException {
		VariableConfig<ScopedVariableHandler> varConfig = new VariableConfig<ScopedVariableHandler>();
		varConfig.add(new VariableInfo<EnabledVariableHandler>(providerName, "true", EnabledVariableHandler.class));
		varConfig.add(new VariableInfo<ConfigVariableHandler>(providerName, "testConfig", ConfigVariableHandler.class));
		return varConfig;
	}

	@Override
	public boolean isEnabled() {
		return isEnabled;
	}

	@Override
	public void setEnabled(boolean parseBoolean) throws PEException {
		SiteProviderContextForProvision ctxt = new SiteProviderContextForProvision(getProviderName());
		try {
			ctxt.setProviderEnabled(parseBoolean);
			this.isEnabled = parseBoolean;
		} finally {
			ctxt.close();
		}
	}

	public String getProviderConfig() {
		return configString;
	}

	public void setProviderConfig(String providerConfig) throws PEException {
		SiteProviderContextForProvision ctxt = new SiteProviderContextForProvision(getProviderName());
		try {
			ctxt.setProviderConfig(providerConfig);
			this.configString = providerConfig;
		} finally {
			ctxt.close();
		}
		this.configString = providerConfig;
	}

	@Override
	public String getProviderName() {
		return providerName;
	}

	@Override
	public void returnSitesByClass(SiteProviderContext ctxt, String siteClass,
			Collection<? extends StorageSite> sites) {
		providerConfig.returnSitesByClass(siteClass, sites);
	}

	@Override
	public void provisionWorkerRequest(SiteProviderContext ctxt,
			GetWorkerRequest req) throws PEException {
		String siteClass = req.getSiteClassName();
		int siteCount = req.getSiteCount();
		req.fulfillGetWorkerRequest(providerConfig.getSitesByClass(siteClass, siteCount));
	}

	@Override
	public List<CatalogEntity> show(SiteManagerCommand smc) throws PEException {
		return null;
	}

	@Override
	public SiteManagerCommand prepareUpdate(SiteManagerCommand smc)
			throws PEException {
		return null;
	}

	@Override
	public int update(SiteManagerCommand smc) throws PEException {
		return 0;
	}

	@Override
	public void rollback(SiteManagerCommand smc) throws PEException {
	}
	

	// To generate an example StaticSiteManager configuration file
	public static void main(String[] args) {
		String[] classes = new String[] { "agg", "sm", "med", "lrg" };
		SiteProviderConfig smc = new SiteProviderConfig();
	
		for (String className : classes) {
			SiteClass sc = new SiteClass(className, 1);
			sc.allSites = new LinkedList<SiteInfo>();
			for (int i = 1; i < 6; ++i) {
				SiteInfo si = new SiteInfo(className + "Stat" + i, "jdbc:mysql://localhost");
				sc.allSites.add(si);
			}
			smc.addSiteClass(sc);
		}
	
		try {
			JAXBContext jc = JAXBContext.newInstance(SiteProviderConfig.class);
			Marshaller m = jc.createMarshaller();
			m.setProperty( Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE );
			m.marshal(smc, new File("providerSample.xml"));
			
			new StaticSiteProvider();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected StaticSiteProvider getProvider(String providerName) throws PEException {
		return (StaticSiteProvider) SiteProviderFactory.getInstance(providerName);
	}

	class EnabledVariableHandler extends ScopedVariableHandler {
		
		@Override
		public void setValue(String scopeName, String name, String value) throws PEException {
			getProvider(scopeName).setEnabled(Boolean.parseBoolean(value));
		}

		@Override
		public String getValue(String scopeName, String name) throws PEException {
			return Boolean.toString(getProvider(scopeName).isEnabled());
		}
		
	}

	class ConfigVariableHandler extends ScopedVariableHandler {

		@Override
		public void setValue(String scopeName, String name, String value) throws PEException {
			getProvider(scopeName).setProviderConfig(value);
		}

		@Override
		public String getValue(String scopeName, String name) throws PEException {
			return getProvider(scopeName).getProviderConfig();
		}
		
	}
	
	@Override
	public Collection<? extends StorageSite> getAllSites() {
		return null;
	}

}
