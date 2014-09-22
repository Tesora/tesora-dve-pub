package com.tesora.dve.common.catalog;

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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.DBNative.DBNativeFactory;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.siteprovider.onpremise.jaxb.OnPremiseSiteProviderConfig;
import com.tesora.dve.sql.transexec.CatalogHelper;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.variables.VariableHandler;

public class TestCatalogHelper extends CatalogHelper {

	private static TestCatalogHelper instance = null;

	public TestCatalogHelper(Class<?> bootClass) throws PEException {
		super(bootClass);
	}

	/**
	 * Get a <code>Properties</code> object containing the PE Project Catalog
	 * properties. Properties file is loaded once and then the
	 * <code>Properties</code> object is store in a static variable
	 * 
	 * @param bootClass
	 * 
	 * @return <code>Properties</code> object
	 * @throws PEException
	 *             if catalog properties file can't be loaded
	 */
	public static Properties getTestCatalogProps(Class<?> bootClass) throws PEException {
		return getInstance(bootClass).getProperties();
	}

	public static TestCatalogHelper getInstance(Class<?> bootClass) throws PEException {
		if (instance == null)
			instance = new TestCatalogHelper(bootClass);

		return instance;
	}

	public static TestCatalogHelper getInstance() {
		return instance;
	}

	/**
	 * Create a fresh DVE Project Catalog database based on a catalog properties
	 * file. An existing database with same name will be DROPped. The
	 * .properties file must contain: driverClass: - e.g., com.mysql.jdbc.Driver
	 * instanceURL: - e.g., jdbc:mysql://localhost dbName: - e.g., test_catalog
	 * userid: - e.g., root password: - e.g., password
	 * 
	 * NOTE: It is expected that a DVE server isn't running when this method is
	 * called. i.e. it should be called before BootstrapHost.startServices().
	 * 
	 * @param bootClass
	 * 
	 * @throws PEException
	 *             if catalog properties file can't be loaded
	 * @throws SQLException
	 */
	public static void createMinimalCatalog(Class<?> bootClass) throws PEException, SQLException {
		TestCatalogHelper helper = null;

		try {
			helper = new TestCatalogHelper(bootClass);

			helper.createMinimalCatalog();
		} finally {
			if (helper != null) {
				helper.close();
				helper = null;
			}
		}
	}

	private void createMinimalCatalog() throws PEException {
		recreateCatalogDB();

		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();
			createMinimalCatalog(c, getCatalogUser(), getCatalogPassword());
			c.commit();
		} finally {
			c.close();
		}
	}

	/**
	 * Brute force recreate the catalog database. Any existing catalog will be
	 * blown away
	 * 
	 * @throws PEException
	 */
	protected void recreateCatalogDB() throws PEException {

		CatalogDAOFactory.shutdown();

		getDBNative();

		DBHelper dbHelper = createDBHelper();

		try {
			dbHelper.connect();

			// DROP/CREATE the database
			dbHelper.executeQuery(dbNative.getDropDatabaseStmt(DBHelper.getConnectionCharset(dbHelper), getCatalogDBName()).getSQL());

			final String catalogName = getCatalogDBName();
			final String defaultScharSet = dbNative.getDefaultServerCharacterSet();
			final String defaultCollation = dbNative.getDefaultServerCollation();
			dbHelper.executeQuery(dbNative
					.getCreateDatabaseStmt(DBHelper.getConnectionCharset(dbHelper), catalogName, false, defaultScharSet, defaultCollation).getSQL());
		} catch (SQLException e) {
			throw new PEException("Error recreating catalog - " + e.getMessage(), e);
		} finally {
			dbHelper.disconnect();
		}
	}

	/**
	 * Create a Test Catalog that has no persistent sites or groups pre-defined
	 * 
	 * NOTE: It is expected that a DVE server isn't running when this method is
	 * called. i.e. it should be called before BootstrapHost.startServices().
	 * 
	 * @throws PEException
	 *             if catalog properties file can't be loaded
	 * @throws SQLException
	 */
	public static void createTestCatalog(Class<?> bootClass) throws PEException {
		TestCatalogHelper helper = null;

		try {
			helper = new TestCatalogHelper(bootClass);

			helper.createTestCatalog();
		} finally {
			if (helper != null) {
				helper.close();
				helper = null;
			}
		}
	}

	/**
	 * Populate an empty DVE Project Catalog with some initial settings - one
	 * Persistent Group ("DefaultGroup") with <code>numSites</code> Persistent
	 * Sites ("site1"..."siteN"). - one UserDatabase ("DefaultDB")
	 * 
	 * NOTE: It is expected that a DVE server isn't running when this method is
	 * called. i.e. it should be called before BootstrapHost.startServices().
	 * 
	 * @param bootClass
	 * 
	 * @param numSites
	 *            - number of StorageSites to create
	 * @throws PEException
	 *             if properties file can't be loaded
	 */
	public static void createTestCatalog(Class<?> bootClass, int numSites) throws PEException {
		createTestCatalog(bootClass, numSites, null, null);
	}

	public static void createTestCatalog(Class<?> bootClass, int numSites, String rootUser, String rootPassword) throws PEException {
		TestCatalogHelper helper = null;
		
		try {
			helper = new TestCatalogHelper(bootClass);

			if(rootUser == null)
				helper.createTestCatalogWithDB(numSites, true);
			else
				helper.createTestCatalogWithDB(numSites, true, rootUser, rootPassword);

			if (numSites != -1) {
				// add 10 - numSites more StorageSites
				for (int i = numSites + 1; i <= 10; i++) {
					helper.createStorageSite("site" + i, helper.getCatalogUrl(), 
							helper.getCatalogUser(), helper.getCatalogPassword());
				}
			}

		} finally {
			if (helper != null) {
				helper.close();
				helper = null;
			}
		}
	}

	public PersistentSite createStorageSite(String name, String url, String user, String password) throws PEException {
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();
			PersistentSite site = c.createPersistentSite(name, url, user, password);
			c.commit();
			return site;
		} finally {
			c.close();
		}
	}

	// -------------------------------------------------------------------------
	// TEST Methods
	//

	/**
	 * Create a catalog for testing purposes. This is the same as creating a
	 * minimal catalog and then defining the default policy and default site
	 * providers
	 * 
	 * @throws PEException
	 */
	public void createTestCatalog() throws PEException {
		recreateCatalogDB();

		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();
			createTestCatalog(c);
			c.commit();
		} finally {
			c.close();
		}
	}

	private Project createTestCatalog(CatalogDAO c) throws PEException {
		return createTestCatalog(c, getCatalogUser(), getCatalogPassword()).getFirst();
	}

	private Pair<Project,Map<VariableHandler,VariableConfig>> createTestCatalog(CatalogDAO c, String user, String password) throws PEException {
		Pair<Project,Map<VariableHandler,VariableConfig>> minimal = 
				createMinimalCatalog(c,user,password);
		Project p = minimal.getFirst();

		// Create a default dynamic site policy
		addTestDefaultDynamicPolicy(minimal, c);

		// Register the built in Providers
		addTestDefaultSiteProviders(c);

		// TODO - should we validate that the dynamic policy is referencing
		// providers that actually exist and that the site providers are using
		// siteClasses that are specified in the policies ?

		return minimal;
	}

	private PersistentGroup createTestStorageGroup(CatalogDAO c, String name, int numStorageSites, String prefix)
			throws PEException {

		// Create the required persistent sites
		List<PersistentSite> sites = new ArrayList<PersistentSite>();

		for (int i = 1; i <= numStorageSites; i++) {
			String siteName = prefix + "site" + i;
			sites.add(createStorageSite(siteName, getCatalogUrl(), getCatalogUser(), getCatalogPassword()));
		}

		// Create a persistent group and add the sites created above
		return createStorageGroup(c, name, sites);
	}

	/**
	 * Create a catalog for testing purposes that has persistent sites and a
	 * default persistent group and also has an optional default database.
	 * 
	 * @param numStorageSites
	 * @throws PEException
	 */
	public void createTestCatalogWithDB(int numStorageSites, boolean withDB) throws PEException {
		createTestCatalogWithDB(numStorageSites, withDB, getCatalogUser(), getCatalogPassword());
	}

	public void createTestCatalogWithDB(int numStorageSites, boolean withDB, String user, String password) throws PEException {
		recreateCatalogDB();
		CatalogDAO c = CatalogDAOFactory.newInstance(catalogProperties);

		try {
			c.begin();

			Pair<Project,Map<VariableHandler,VariableConfig>> minimal = createTestCatalog(c, user, password); 

			PersistentGroup sg = createTestStorageGroup(c, PEConstants.DEFAULT_GROUP_NAME, numStorageSites, "");
			
			minimal.getSecond().get(KnownVariables.PERSISTENT_GROUP).setValue(sg.getName());

			if (withDB) {
				DBNative dbn = DBNativeFactory.newInstance(DBType.fromDriverClass(catalogProperties
						.getProperty(PEConstants.PROP_FULL_JDBC_DRIVER)));
				c.createDatabase(UserDatabase.DEFAULT, sg, dbn.getDefaultServerCharacterSet(),
						dbn.getDefaultServerCollation());
			}

			c.commit();
		} finally {
			c.close();
		}
	}

	private int siteCount = 5;

	private DynamicPolicy addTestDefaultDynamicPolicy(Pair<Project,Map<VariableHandler,VariableConfig>> minimal, CatalogDAO c) throws PEException {
		try {
			DynamicPolicy policy = generatePolicyConfig(siteCount, OnPremiseSiteProvider.DEFAULT_NAME);

			c.persistToCatalog(policy);

			minimal.getSecond().get(KnownVariables.DYNAMIC_POLICY).setValue(policy.getName());

			return policy;
		} catch (Throwable t) {
			throw new PEException("Cannot create Dynamic Policy: " + t.getMessage(), t);
		}
	}

	private OnPremiseSiteProviderConfig addTestDefaultSiteProviders(CatalogDAO c) throws PEException {
		OnPremiseSiteProviderConfig config = generateProviderConfig(siteCount, OnPremiseSiteProvider.DEFAULT_NAME, getCatalogUrl(),
				getCatalogUser(), getCatalogPassword());

		c.createProvider(OnPremiseSiteProvider.DEFAULT_NAME, OnPremiseSiteProvider.class.getCanonicalName(), PEXmlUtils.marshalJAXB(config));
		
		return config;
	}
}
