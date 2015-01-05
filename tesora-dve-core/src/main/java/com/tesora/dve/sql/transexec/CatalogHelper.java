package com.tesora.dve.sql.transexec;

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



import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.tesora.dve.charset.NativeCharSetCatalogImpl;
import com.tesora.dve.common.catalog.*;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.infoschema.spi.CatalogGenerator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PECryptoUtils;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.ContainerDistributionModel;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.InsertEngine;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.siteprovider.onpremise.jaxb.OnPremiseSiteProviderConfig;
import com.tesora.dve.siteprovider.onpremise.jaxb.PoolConfig;
import com.tesora.dve.sql.infoschema.InformationSchemas;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.VariableManager;

public class CatalogHelper {
	private static final String DEFAULT_ACCESSSPEC = "%";
	public static final String DEFAULT_SITE_PREFIX = "site";

	protected Properties catalogProperties;
	protected DBNative dbNative;

	private String rootUser;
	private String rootPassword;

	public CatalogHelper(Class<?> bootClass) throws PEException {
		catalogProperties = PEFileUtils.loadPropertiesFile(bootClass, PEConstants.CONFIG_FILE_NAME);

		// Attempt to load additional properties from server.properties and if
		// available then merge them into the properties we loaded from dve.properties
		try {
			catalogProperties.putAll(PEFileUtils.loadPropertiesFromClasspath(bootClass, PEConstants.SERVER_FILE_NAME));
		} catch (Exception e) {
			// Eat the exception
		}
	}

	public CatalogHelper(Properties props) throws PEException {
		catalogProperties = props;
	}

	public Properties getProperties() {
		return catalogProperties;
	}

	/**
	 * Same as the connection URL but without any parameters.
	 */
	public String getCatalogDatabaseLocation() throws PEException {
		final PEUrl baseUrl = this.buildCatalogDatabaseConnectionUrl();
		baseUrl.clearQuery();
		return baseUrl.toString();
	}

	public String getCatalogDatabaseConnectionUrl() throws PEException {
		return this.buildCatalogDatabaseConnectionUrl().toString();
	}

	private PEUrl buildCatalogDatabaseConnectionUrl() throws PEException {
		final PEUrl baseUrl = PEUrl.fromUrlString(this.getCatalogBaseUrl());
		baseUrl.setPath(this.getCatalogDBName());
		return baseUrl;
	}

	public String getCatalogBaseUrl() throws PEException {
		return CatalogURL.buildCatalogBaseUrlFrom(catalogProperties.getProperty(DBHelper.CONN_URL)).toString();
	}

	public String getCatalogDBName() throws PEException {
		return catalogProperties.getProperty(DBHelper.CONN_DBNAME, PEConstants.CATALOG);
	}

	public String getCatalogUser() throws PEException {
		return catalogProperties.getProperty(DBHelper.CONN_USER);
	}

	public String getCatalogPassword() throws PEException {
		return catalogProperties.getProperty(DBHelper.CONN_PASSWORD);
	}

	public void close() {
		CatalogDAOFactory.shutdown();
	}

	public void setRootCredentials(String user, String password) {
		this.rootUser = user;
		this.rootPassword = password;
	}

	public String getRootUser() throws PEException {
		if (rootUser == null)
			return getCatalogUser();

		return rootUser;
	}

	public String getRootPassword() throws PEException {
		if (rootPassword == null)
			return getCatalogPassword();

		return rootPassword;
	}

	/**
	 * Create a new Catalog supporting a Single Host deployment consisting of a
	 * number of persistent sites and a number of dynamic sites all on
	 * localhost. All storage sites and dynamic sites will use the url / user /
	 * password from the catalogProperties.
	 * 
	 * @param providerName
	 *            Name of site provider
	 * @param sgName
	 *            Name of storage group
	 * @param storageSites
	 *            Number of storage sites
	 * @param dynamicSites
	 *            Number of dynamic sites
	 * @throws PEException
	 */
	@SuppressWarnings("rawtypes")
	public void createLocalhostCatalog(String providerName, String sgName, int storageSites, int dynamicSites)
			throws PEException {

		createCatalogDB();

		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			Pair<Project,Map<VariableHandler,VariableConfig>> minimal = 
					createMinimalCatalog(c, getRootUser(), getRootPassword());

			// Generate a Site Provider configuration with encrypted passwords
			OnPremiseSiteProviderConfig providerConfig = generateProviderConfig(dynamicSites, providerName,
					getCatalogBaseUrl(), getCatalogUser(), getCatalogPassword());

			c.createProvider(providerName, OnPremiseSiteProvider.class.getCanonicalName(),
					PEXmlUtils.marshalJAXB(providerConfig));

			// Generate a Dynamic Policy that matches the Site Provider created
			// above
			DynamicPolicy policy = generatePolicyConfig(dynamicSites, providerName);
			c.persistToCatalog(policy);

			// Set this policy as default
			minimal.getSecond().get(KnownVariables.DYNAMIC_POLICY).setValue(policy.getName());

			// Create a persistent group with the required number of persistent
			// sites on the catalog host
			List<PersistentSite> sites = new ArrayList<PersistentSite>();
			for (int i = 1; i <= storageSites; i++) {
				sites.add(c.createPersistentSite(DEFAULT_SITE_PREFIX + i, getCatalogBaseUrl(), getCatalogUser(),
						getCatalogPassword()));
			}
			PersistentGroup sg = createStorageGroup(c, sgName, sites);

			// Set this persistent group as the default
			minimal.getSecond().get(KnownVariables.PERSISTENT_GROUP).setValue(sg.getName());

			c.commit();
		} finally {
			c.close();
		}
	}

	@SuppressWarnings("rawtypes")
	public void createBootstrapCatalog() throws PEException {

		createCatalogDB();

		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			Pair<Project,Map<VariableHandler,VariableConfig>> minimal = 
					createMinimalCatalog(c, getRootUser(), getRootPassword());
			// Generate a Site Provider configuration with encrypted passwords
			OnPremiseSiteProviderConfig providerConfig = generateProviderConfig(1, PEConstants.BOOTSTRAP_PROVIDER_NAME,
					getCatalogBaseUrl(), getCatalogUser(), getCatalogPassword());

			c.createProvider(PEConstants.BOOTSTRAP_PROVIDER_NAME, OnPremiseSiteProvider.class.getCanonicalName(),
					PEXmlUtils.marshalJAXB(providerConfig));

			// Generate a Dynamic Policy that matches the Site Provider created
			// above
			DynamicPolicy policy = generatePolicyConfig(1, PEConstants.BOOTSTRAP_PROVIDER_NAME);
			c.persistToCatalog(policy);

			// Set this policy as default
			minimal.getSecond().get(KnownVariables.DYNAMIC_POLICY).setValue(policy.getName());

			c.commit();
		} finally {
			c.close();
		}
	}
	
	@SuppressWarnings("rawtypes")
	public void createStandardCatalog(List<PersistentSite> sites, List<PersistentGroup> groups,
			PersistentGroup defaultGroup, OnPremiseSiteProviderConfig dynamic, DynamicPolicy policy) throws PEException {

		createCatalogDB();

		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			Pair<Project,Map<VariableHandler,VariableConfig>> minimal = 
					createMinimalCatalog(c, getRootUser(), getRootPassword());

			// We are going to do the policy first since we need the name
			// of the dynamic provider out of the policy
			c.persistToCatalog(policy);

			// Set this policy as default
			minimal.getSecond().get(KnownVariables.DYNAMIC_POLICY).setValue(policy.getName());

			// For now just grab the provider name from the aggregation class
			String providerName = policy.getAggregationClass().getProvider();

			// For now we will hardcode this to use the OnPremiseSiteProvider
			c.createProvider(providerName, OnPremiseSiteProvider.class.getCanonicalName(),
					PEXmlUtils.marshalJAXB(dynamic));

			// First create the persistent sites
			if (sites != null) {
				for (PersistentSite site : sites) {
					if (c.findPersistentSite(site.getName(), false) != null)
						throw new PEException("Persistent site '" + site.getName() + "' already exists");

					for (SiteInstance inst : site.getSiteInstances()) {
						if (c.findSiteInstance(inst.getName(), false) != null)
							throw new PEException("Site instance '" + inst.getName() + "' already exists");
					}

					c.persistToCatalog(site);

					for (SiteInstance inst : site.getSiteInstances()) {
						c.persistToCatalog(inst);
					}
				}
			}

			// then the persistent groups
			if (groups != null) {
				for (PersistentGroup group : groups) {
					if (c.findPersistentGroup(group.getName(), false) != null)
						throw new PEException("Persistent group '" + group.getName() + "' already exists");
					c.persistToCatalog(group);
				}
			}

			if (defaultGroup != null)
				minimal.getSecond().get(KnownVariables.PERSISTENT_GROUP).setValue(defaultGroup.getName());

			c.commit();
		} finally {
			c.close();
		}
	}

	public void deleteCatalog() throws PEException {

		CatalogDAOFactory.shutdown();

		getDBNative();

		DBHelper dbHelper = createDBHelper();

		try {
			dbHelper.connect();
			dbHelper.executeQuery(dbNative.getDropDatabaseStmt(DBHelper.getConnectionCharset(dbHelper), getCatalogDBName()).getSQL());
		} catch (SQLException e) {
			throw new PEException("Error deleting DVE catalog - " + e.getMessage(), e);
		} finally {
			dbHelper.disconnect();
		}
	}

	@SuppressWarnings("rawtypes")
	protected Pair<Project,Map<VariableHandler,VariableConfig>> createMinimalCatalog(CatalogDAO c, String user, String password) throws PEException {
		createSchema(c);

		// Create the distribution models
		RandomDistributionModel rdm = new RandomDistributionModel();
		c.persistToCatalog(rdm);
		c.persistToCatalog(new BroadcastDistributionModel());
		c.persistToCatalog(new StaticDistributionModel());
		c.persistToCatalog(new RangeDistributionModel());
		c.persistToCatalog(new ContainerDistributionModel());

		// Create the Default Project
		Project project = c.createProject(Project.DEFAULT);

		project.setRootUser(c.createUser(user, password, DEFAULT_ACCESSSPEC, true));

		// set up the variables, so that we can indicate the default group/policy
		Map<VariableHandler,VariableConfig> variables = VariableManager.getManager().initializeCatalog(c);
		
		// load the information schema
		PersistentGroup infoSchemaGroup = c.createPersistentGroup(PEConstants.INFORMATION_SCHEMA_GROUP_NAME);

		// create the System persistent site and persistent group
		PersistentGroup sysSG = c.createPersistentGroup(PEConstants.SYSTEM_GROUP_NAME);
		sysSG.addStorageSite(c.createPersistentSite(PEConstants.SYSTEM_SITENAME, getCatalogBaseUrl(), getCatalogUser(),
				getCatalogPassword()));

		// load the default character sets
		String driver = DBHelper.loadDriver(catalogProperties.getProperty(DBHelper.CONN_DRIVER_CLASS));
		NativeCharSetCatalog csCatalog = NativeCharSetCatalogImpl.getDefaultCharSetCatalog(DBType.fromDriverClass(driver));
		DBNative.saveToCatalog(c, csCatalog);

		// Create the engines
		for (Engines engines : Engines.getDefaultEngines()) {
			c.persistToCatalog(engines);
		}

		InformationSchemas schema = InformationSchemas.build(dbNative,c,catalogProperties);
		// info schema is now persisted directly to facilitate upgrades
		List<PersistedEntity> ents = schema.buildEntities(infoSchemaGroup.getId(), rdm.getId(),
				dbNative.getDefaultServerCharacterSet(), dbNative.getDefaultServerCollation());
		InsertEngine ie = new InsertEngine(ents, new DAOPersistProvider(c));
		ie.populate();

		return new Pair<Project,Map<VariableHandler,VariableConfig>>(project,variables);
	}

	protected void createSchema(CatalogDAO c) throws PEException {
		CatalogGenerator generator = Singletons.require(CatalogGenerator.class);
		generator.installCurrentSchema(c, catalogProperties);
	}
	
	public void deleteAllServerRegistration() throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();
			c.deleteAllServerRegistration();
			c.commit();
		} finally {
			c.close();
		}
	}
	
	// -------------------------------------------------------------------------

	public void createTemplate(String name, String template, String match, String comment) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			if (c.findTemplate(name, false) != null)
				throw new PEException("Template '" + name + "' already exists in the catalog");

			c.createTemplate(name, template, match, comment);

			c.commit();
		} finally {
			c.close();
		}
	}
	
	public void updateTemplate(String name, String template, String match, String comment) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			PersistentTemplate t = c.findTemplate(name,  false);

			if (t == null)
				throw new PEException("Template '" + name + "' doesn't exist in the catalog");

			t.setDefinition(template);
			t.setMatch(match);
			t.setComment(comment);

			c.commit();
		} finally {
			c.close();
		}
	}

	// -------------------------------------------------------------------------

	protected PersistentGroup createStorageGroup(CatalogDAO c, String sgName, List<PersistentSite> sites)
			throws PEException {
		PersistentGroup sg = c.createPersistentGroup(sgName);
		sg.addAllSites(sites);
		return sg;
	}

	public PersistentGroup setDefaultStorageGroup(String name) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			PersistentGroup sg = c.findPersistentGroup(name, false);

			if (sg == null)
				throw new PEException("Persistent Group '" + name + "' not found in the catalog");

			c.begin();
			VariableConfig vc = KnownVariables.PERSISTENT_GROUP.lookupPersistentConfig(c);
			vc.setValue(sg.getName());
			c.commit();

			return sg;
		} finally {
			c.close();
		}
	}

	public PersistentGroup getStorageGroup(String name) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			PersistentGroup sg = c.findPersistentGroup(name, false);

			if (sg == null)
				throw new PEException("Persistent Group '" + name + "' not found in the catalog");

			// call getStorageSites() to populate the group object - since they
			// are lazily loaded
			sg.getStorageSites();

			return sg;
		} finally {
			c.close();
		}
	}

	// -------------------------------------------------------------------------
	// DYNAMIC SITE PROVIDER METHODS
	//

	public Provider createSiteProvider(String name, String plugin, String config) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			if (c.findProvider(name, false) != null)
				throw new PEException("Site Provider '" + name + "' already exists in the catalog");

			c.begin();

			Provider provider = c.createProvider(name, plugin);
			if (config != null)
				provider.setConfig(StringEscapeUtils.escapeSql(config));

			c.commit();

			return provider;
		} finally {
			c.close();
		}
	}

	public Provider getSiteProvider(String name) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			Provider p = c.findProvider(name, false);

			if (p == null)
				throw new PEException("Site Provider '" + name + "' not found in the catalog");
			return p;
		} finally {
			c.close();
		}
	}

	public void removeSiteProvider(String name) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			Provider provider = c.findProvider(name, false);

			if (provider == null)
				throw new PEException("Site Provider '" + name + "' doesn't exist in the catalog");

			c.remove(provider);

			c.commit();
		} finally {
			c.close();
		}
	}

	public Provider setSiteProviderConfig(String name, String config) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			Provider provider = c.findProvider(name, false);

			if (provider == null)
				throw new PEException("Site Provider '" + name + "' not found in the catalog");

			provider.setConfig(StringEscapeUtils.escapeSql(config));
			c.commit();

			return provider;
		} finally {
			c.close();
		}
	}

	// -------------------------------------------------------------------------
	// DYNAMIC POLICY METHODS
	//
	public DynamicPolicy getDynamicPolicy(String name) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			DynamicPolicy p = c.findDynamicPolicy(name, false);

			if (p == null)
				throw new PEException("Dynamic Policy '" + name + "' not found in the catalog");

			return p;
		} finally {
			c.close();
		}
	}

	public DynamicPolicy createDynamicPolicy(DynamicPolicy policy) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			if (c.findDynamicPolicy(policy.getName(), false) != null)
				throw new PEException("Dynamic Policy '" + policy.getName() + "' already exists in the catalog");

			c.persistToCatalog(policy);

			c.commit();

			return policy;
		} finally {
			c.close();
		}
	}

	public DynamicPolicy setDynamicPolicyConfig(DynamicPolicy policy) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			DynamicPolicy p = c.findDynamicPolicy(policy.getName(), false);

			if (p == null) {
				c.persistToCatalog(policy);
				p = policy;
			} else {
				p.take(policy);
			}
			c.commit();

			return p;
		} finally {
			c.close();
		}
	}

	public DynamicPolicy setDefaultDynamicPolicy(String name) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			DynamicPolicy policy = c.findDynamicPolicy(name, false);

			if (policy == null)
				throw new PEException("Dynamic Policy '" + name + "' not found in the catalog");

			VariableConfig vc = KnownVariables.DYNAMIC_POLICY.lookupPersistentConfig(c);
			vc.setValue(policy.getName());
			
			c.commit();

			return policy;
		} finally {
			c.close();
		}
	}

	// -------------------------------------------------------------------------
	// VARIABLES METHODS
	//

	public List<VariableConfig> getAllVariables() throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			return c.findAllVariableConfigs();
		} finally {
			c.close();
		}
	}

	@SuppressWarnings("rawtypes")
	public VariableConfig setVariable(String key, String value, boolean create) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {

			VariableConfig config = c.findVariableConfig(key, false);

			if (config == null) {
				if (!create)
					throw new PEException("Variable '" + key + "' not found in the catalog");

				
				try {
					VariableHandler handler = VariableManager.getManager().lookupMustExist(null,key);
					VariableConfig vc = handler.buildNewConfig();
					vc.setValue(value);
					c.persistToCatalog(vc);
					return vc;
				} catch (Throwable th) {
					throw new PEException("Failed to create variable '" + key + "'", th);
				}
			} else {
				c.begin();

				config.setValue(value);

				c.commit();
			}
			return config;
		} finally {
			c.close();
		}
	}
	
	// -------------------------------------------------------------------------
	// EXTERNAL SERVICES METHODS
	//
	public ExternalService createExternalService(String name, String plugin, String connectUser, boolean usesDataStore,
			String config) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			if (c.findExternalService(name, false) != null)
				throw new PEException("External Service '" + name + "' already exists in the catalog");

			ExternalService es = c.createExternalService(name, plugin, connectUser, usesDataStore);
			if (config != null)
				es.setConfig(StringEscapeUtils.escapeSql(config));

			c.commit();

			return es;
		} finally {
			c.close();
		}
	}

	public ExternalService getExternalService(String name) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			ExternalService es = c.findExternalService(name, false);

			if (es == null)
				throw new PEException("External Service '" + name + "' not found in the catalog");
			return es;
		} finally {
			c.close();
		}
	}

	public void removeExternalService(String name) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);
		// TODO - will need to remove the service datastore
		try {
			c.begin();

			ExternalService es = c.findExternalService(name, false);

			if (es == null)
				throw new PEException("External Service '" + name + "' doesn't exist in the catalog");

			c.remove(es);

			c.commit();
		} finally {
			c.close();
		}
	}

	public ExternalService setExternalServiceConfig(String name, String config) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			ExternalService es = c.findExternalService(name, false);

			if (es == null)
				throw new PEException("External Service '" + name + "' not found in the catalog");

			es.setConfig(StringEscapeUtils.escapeSql(config));
			c.commit();

			return es;
		} finally {
			c.close();
		}
	}

	// -------------------------------------------------------------------------

	public void checkURLAvailable() throws PEException {
		// In production we don't have a default value for the catalog URL
		if (StringUtils.isEmpty(this.getCatalogBaseUrl()))
			throw new PEException("Value for " + DBHelper.CONN_URL + " not specified in properties");
	}

	protected DBNative getDBNative() throws PEException {
		if (dbNative == null) {
			checkURLAvailable();

			dbNative = DBNative.DBNativeFactory.newInstance(DBHelper.urlToDBType(getCatalogBaseUrl()));
		}
		return dbNative;
	}

	protected DBHelper createDBHelper() throws PEException {
		checkURLAvailable();

		Properties props = (Properties) catalogProperties.clone();
		props.remove(DBHelper.CONN_DBNAME);

		return new DBHelper(props);
	}

	public boolean catalogExists(DBHelper helper) {
		try {
			helper.executeQuery("SELECT name FROM dve_catalog.project");

			return true;
		} catch (Exception e) {
		}
		return false;
	}

	/**
	 * Create a new catalog database. This method will fail if a catalog already
	 * exists
	 * 
	 * @throws PEException
	 */
	public void createCatalogDB() throws PEException {
		// Shutdown any existing CatlogDAO
		CatalogDAOFactory.shutdown();

		getDBNative();

		DBHelper dbHelper = createDBHelper();

		try {
			dbHelper.connect();

			if (catalogExists(dbHelper))
				throw new PEException("DVE Catalog already exists at '" + getCatalogDatabaseLocation() + "'");

			final String catalogName = getCatalogDBName();
			final String defaultScharSet = dbNative.getDefaultServerCharacterSet();
			final String defaultCollation = dbNative.getDefaultServerCollation();
			dbHelper.executeQuery(dbNative
					.getCreateDatabaseStmt(DBHelper.getConnectionCharset(dbHelper), catalogName, false, defaultScharSet, defaultCollation).getSQL());
		} catch (SQLException e) {
			throw new PEException("Error creating DVE catalog - " + e.getMessage(), e);
		} finally {
			if (dbHelper != null)
				dbHelper.disconnect();
		}
	}

	public void dumpCatalogInfo(PrintWriter pw) throws PEException {
		dumpCatalogInfo(pw, false);
	}

	public void dumpCatalogInfo(PrintWriter pw, boolean verbose) throws PEException {
		pw.println("URL: " + getCatalogDatabaseLocation());

		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);
		try {
			Project p = c.findDefaultProject();

			VariableConfig dpgvc = KnownVariables.PERSISTENT_GROUP.lookupPersistentConfig(c); 
			VariableConfig dpvc = KnownVariables.DYNAMIC_POLICY.lookupPersistentConfig(c); 
			
			if (p != null) {
				pw.println("Default project found in catalog");
				pw.println("    Default Persistent Group = "
						+ (dpgvc.getValue() != null ? dpgvc.getValue() : "not set"));
				pw.println("    Default Policy Group     = "
						+ (dpvc.getValue() != null ? dpvc.getValue() : "not set"));
				pw.println("    Root User                = "
						+ (p.getRootUser() != null ? p.getRootUser().getName() : "not set"));
			} else {
				pw.println("No Default project found in catalog");
			}
			pw.println("Catalog contains:");
			pw.println("    Users          (" + c.findAllUsers().size() + ")");
			pw.println("    Tenants        (" + c.findAllTenants().size() + ")");
			pw.println("    User Databases (" + c.findAllUserDatabases().size() + ")");
			pw.println("    User Tables    (" + c.findAllUserTables().size() + ")");
			pw.println("    User Columns   (" + c.findAllUserColumns().size() + ")");
			pw.println("    Distribution Models (" + c.findAllDistributionModels().size() + ")");
			if (verbose) {
				for (DistributionModel model : c.findAllDistributionModels())
					pw.println("        " + model.getName());
			}
			pw.println("    Persistent Groups (" + c.findAllPersistentGroups().size() + ")");
			if (verbose) {
				for (PersistentGroup group : c.findAllPersistentGroups()) {
					pw.println("        " + group.getName() + " (sites=" + group.getStorageSites().size() + ")");
					for (PersistentSite site : group.getStorageSites()) {
						pw.println("            " + site.getName());
					}
				}
			}
			pw.println("    Persistent Sites (" + c.findAllPersistentSites().size() + ")");
			if (verbose) {
				for (PersistentSite site : c.findAllPersistentSites()) {
					pw.println("        " + site.getName() + " (ha_type=" + site.getHAType() + ")");
					for (SiteInstance inst : site.getSiteInstances()) {
						pw.println("            " + inst.getName() + " (url=" + inst.getInstanceURL() + ", user="
								+ inst.getUser() + ", enabled=" + inst.isEnabled() + ", master=" + inst.isMaster()
								+ ")");
					}
				}
			}
			pw.println("    Dynamic Providers (" + c.findAllProviders().size() + ")");
			if (verbose) {
				for (Provider provider : c.findAllProviders()) {
					pw.println("        Provider=" + provider.getName());
					pw.println("            " + provider.getPlugin() + " (enabled=" + provider.isEnabled() + ")");
				}
			}

			pw.println("    Dynamic Policies (" + c.findAllDynamicPolicies().size() + ")");
			if (verbose) {
				for (DynamicPolicy policy : c.findAllDynamicPolicies()) {
					pw.println("        Policy = " + policy.getName());
					pw.println("            Aggregation = " + policy.getAggregationClass().toString());
					pw.println("            Small       = " + policy.getSmallClass().toString());
					pw.println("            Medium      = " + policy.getMediumClass().toString());
					pw.println("            Large       = " + policy.getLargeClass().toString());
				}
			}
			pw.println("    External Services (" + c.findAllExternalServices().size() + ")");
			if (verbose) {
				for (ExternalService es : c.findAllExternalServices()) {
					pw.println("        Service = " + es.getName());
					pw.println("            Plugin         = " + es.getPlugin());
					pw.println("            Uses DataStore = " + es.usesDataStore());
					pw.println("            Auto Start     = " + es.isAutoStart());
					pw.println("            Connect User   = " + es.getConnectUser());
				}
			}
			pw.println("    Servers (" + c.findAllRegisteredServers().size() + ")");
			if (verbose) {
				for (ServerRegistration s : c.findAllRegisteredServers()) {
					pw.println("        ID = " + s.getId());
					pw.println("            IP Address = " + s.getIpAddress());
					pw.println("            Name       = " + s.getName());
				}
			}
			pw.flush();
			pw.close();
		} finally {
			c.close();
		}
	}

	// ------------------------------------------------------------------------

	private static final String DEFAULT_SITE = "dyn";
	private static final String LOCAL = "LOCAL";

	public static OnPremiseSiteProviderConfig generateProviderConfig(int count, String providerName, String url,
			String user, String password) throws PEException {
		// We need the port from the catalogUrl
		int port = PEUrl.fromUrlString(url).getPort();
		String useUrl = PEUrl.fromUrlString(PEConstants.MYSQL_URL).setPort(port).toString();

		String encryptedPassword = PECryptoUtils.encrypt(password);

		PoolConfig pool = new PoolConfig();
		pool.setName(LOCAL);

		OnPremiseSiteProviderConfig slc = new OnPremiseSiteProviderConfig();
		slc.getPool().add(pool);

		List<PoolConfig.Site> sites = pool.getSite();
		for (int i = 1; i <= count; i++) {
			PoolConfig.Site site = new PoolConfig.Site();
			site.setName(DEFAULT_SITE + i);
			site.setUrl(useUrl);
			site.setUser(user);
			site.setPassword(encryptedPassword);
			site.setMaxQueries(-1);
			sites.add(site);
		}
		return slc;
	}

	public static OnPremiseSiteProviderConfig encryptProviderPasswords(OnPremiseSiteProviderConfig config)
			throws PEException {
		if (config.getPool() != null) {
			for (PoolConfig pool : config.getPool()) {
				if (pool.getSite() != null) {
					for (PoolConfig.Site site : pool.getSite()) {
						String encryptedPassword = PECryptoUtils.encrypt(site.getPassword());
						site.setPassword(encryptedPassword);
					}
				}
			}
		}
		return config;
	}

	public static DynamicPolicy generatePolicyConfig(int count, String providerName) throws PEException {
		int small = (count + 1) / 2;
		int medium = (count + 1) / 2;
		int large = count;

		DynamicPolicy policy = new DynamicPolicy();
		policy.setName(OnPremiseSiteProvider.DEFAULT_POLICY_NAME);
		policy.setAggregationClass(new DynamicGroupClass(DynamicPolicy.AGGREGATION, providerName, LOCAL, 1));
		policy.setSmallClass(new DynamicGroupClass(DynamicPolicy.SMALL, providerName, LOCAL, small));
		policy.setMediumClass(new DynamicGroupClass(DynamicPolicy.MEDIUM, providerName, LOCAL, medium));
		policy.setLargeClass(new DynamicGroupClass(DynamicPolicy.LARGE, providerName, LOCAL, large));
		policy.setStrict(true);

		return policy;
	}
}
