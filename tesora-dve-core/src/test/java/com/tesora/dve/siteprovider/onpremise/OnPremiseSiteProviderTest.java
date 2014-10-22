package com.tesora.dve.siteprovider.onpremise;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.siteprovider.onpremise.jaxb.OnPremiseSiteProviderConfig;
import com.tesora.dve.siteprovider.onpremise.jaxb.PoolConfig;
import com.tesora.dve.siteprovider.onpremise.jaxb.PoolConfig.Site;
import com.tesora.dve.standalone.PETest;

public class OnPremiseSiteProviderTest extends PETest {

	public Logger logger = Logger.getLogger(OnPremiseSiteProviderTest.class);

	public DBHelper dbHelper;
	public static String catalogUrl;
	public static String catalogUser;
	public static String catalogPassword;

	@BeforeClass
	public static void setup() throws Exception {
		TestCatalogHelper.getTestCatalogProps(PETest.class);
		TestCatalogHelper.createTestCatalog(PETest.class,2);

		catalogUrl = PEUrl.stripUrlParameters(TestCatalogHelper.getInstance().getCatalogBaseUrl());
		catalogUser = TestCatalogHelper.getInstance().getCatalogUser();
		catalogPassword = TestCatalogHelper.getInstance().getCatalogPassword();
		
		bootHost = BootstrapHost.startServices(PETest.class);
	}

	@Before
	public void setupUserData() throws Exception {
		Properties peProps = PEFileUtils.loadPropertiesFile(OnPremiseSiteProviderTest.class, PEConstants.CONFIG_FILE_NAME);

		dbHelper = new DBHelper(peProps.getProperty("jdbc.url"), peProps.getProperty("jdbc.user"),
				peProps.getProperty("jdbc.password"));
		dbHelper.connect();
	}

	@After
	public void testCleanup() throws PEException {
		dbHelper.disconnect();
	}

	@Test
	public void providerTest() throws Exception {
		assertTrue(dbHelper.executeQuery("SHOW DYNAMIC SITE PROVIDERS"));

		ResultSet rs = dbHelper.getResultSet();
		rs.next();

		assertEquals("OnPremise", rs.getString(1));
		assertEquals("com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider", rs.getString(2));
		assertEquals(true, rs.getBoolean(3));

		OnPremiseSiteProviderConfig expectedData = new OnPremiseSiteProviderConfig();
		PoolConfig expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);

		validateProviderConfig("OnPremise", expectedData);

		// Test add site
		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='add site' site='test1' pool='LOCAL' url='"
				+ catalogUrl + "' user='" + catalogUser + "' password='" + catalogPassword + "' maxQueries=10");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "test1", catalogUrl, catalogUser, catalogPassword, 10);

		validateProviderConfig("OnPremise", expectedData);

		// Test invalid add site - duplicate site name
		boolean exceptionCaught = false;
		try {
			dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='add site' site='test1' pool='LOCAL' url='"
					+ catalogUrl + "' user='" + catalogUser + "' password='" + catalogPassword + "' maxQueries=10");
		} catch (Exception e) {
			assertEquals("PEException: Site 'test1' in pool 'LOCAL' already exists in provider configuration",
					e.getMessage());
			exceptionCaught = true;
		}
		assertTrue("cmd=add site with duplicate name should throw exception", exceptionCaught);

		// Test invalid add site - non-existent pool

		exceptionCaught = false;
		try {
			dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='add site' site='test2' pool='POOL2' url='"
					+ catalogUrl + "' user='" + catalogUser + "' password='" + catalogPassword + "' maxQueries=10");

		} catch (Exception e) {
			assertEquals("PEException: Pool 'POOL2' does not exist in provider configuration", e.getMessage());
			exceptionCaught = true;
		}
		assertTrue("cmd=add site with unknown pool should throw exception", exceptionCaught);

		// Test alter site

		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='alter site' site='test1' pool='LOCAL' maxQueries=20");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "test1", catalogUrl, catalogUser, catalogPassword, 20);

		validateProviderConfig("OnPremise", expectedData);

		// Test invalid alter site - non existing site name
		exceptionCaught = false;
		try {
			dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='alter site' site='test2' pool='LOCAL' maxQueries=20");
		} catch (Exception e) {
			assertEquals("PEException: Site 'test2' in pool 'LOCAL' does not exist in provider configuration",
					e.getMessage());
			exceptionCaught = true;
		}
		assertTrue("cmd=alter site with non existing name should throw exception", exceptionCaught);

		// Test drop site

		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='drop site' site='test1' pool='LOCAL'");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);

		validateProviderConfig("OnPremise", expectedData);

		// Test invalid drop site - non-existing site name
		exceptionCaught = false;
		try {
			dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='drop site' site='test3' pool='LOCAL'");
		} catch (Exception e) {
			assertEquals("PEException: Site 'test3' in pool 'LOCAL' does not exist in provider configuration",
					e.getMessage());
			exceptionCaught = true;
		}
		assertTrue("cmd=drop site with non existing name should throw exception", exceptionCaught);

		// Test add pool

		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='add pool' pool='REMOTE1'");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);
		expectedPool = addTestPool(expectedData, "REMOTE1");

		validateProviderConfig("OnPremise", expectedData);

		// Add site to new pool
		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='add site' site='remote1' pool='REMOTE1' url='"
				+ catalogUrl + "' user='" + catalogUser + "' password='" + catalogPassword + "' maxQueries=10");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);
		expectedPool = addTestPool(expectedData, "REMOTE1");
		addTestSite(expectedPool, "remote1", catalogUrl, catalogUser, catalogPassword, 10);

		validateProviderConfig("OnPremise", expectedData);

		// Add pool with alternate

		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='add pool' pool='REMOTE2' alternatePool='LOCAL'");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);
		expectedPool = addTestPool(expectedData, "REMOTE1");
		addTestSite(expectedPool, "remote1", catalogUrl, catalogUser, catalogPassword, 10);
		expectedPool = addTestPool(expectedData, "REMOTE2", "LOCAL");

		validateProviderConfig("OnPremise", expectedData);

		// Alter REMOTE1 to have an alternate

		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='alter pool' pool='REMOTE1' alternatePool='REMOTE2'");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);
		expectedPool = addTestPool(expectedData, "REMOTE1", "REMOTE2");
		addTestSite(expectedPool, "remote1", catalogUrl, catalogUser, catalogPassword, 10);
		expectedPool = addTestPool(expectedData, "REMOTE2", "LOCAL");

		validateProviderConfig("OnPremise", expectedData);

		// Alter REMOTE2 to remove alternate

		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='alter pool' pool='REMOTE2' alternatePool=null");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);
		expectedPool = addTestPool(expectedData, "REMOTE1", "REMOTE2");
		addTestSite(expectedPool, "remote1", catalogUrl, catalogUser, catalogPassword, 10);
		expectedPool = addTestPool(expectedData, "REMOTE2");

		validateProviderConfig("OnPremise", expectedData);

		// Drop pool

		dbHelper.executeQuery("ALTER DYNAMIC SITE PROVIDER `OnPremise` cmd='drop pool' pool='REMOTE1'");

		expectedData = new OnPremiseSiteProviderConfig();
		expectedPool = addTestPool(expectedData, "LOCAL");
		addTestSite(expectedPool, "dyn1", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn2", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn3", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn4", catalogUrl, catalogUser, catalogPassword, -1);
		addTestSite(expectedPool, "dyn5", catalogUrl, catalogUser, catalogPassword, -1);
		expectedPool = addTestPool(expectedData, "REMOTE2");

		validateProviderConfig("OnPremise", expectedData);

	}

	private void validateProviderConfig(String provider, OnPremiseSiteProviderConfig expected) throws Exception {
		assertTrue(dbHelper.executeQuery("SHOW DYNAMIC SITE PROVIDER `" + provider + "` cmd='config'"));
		ResultSet rs = dbHelper.getResultSet();
		rs.next();

		OnPremiseSiteProviderConfig actual = PEXmlUtils.unmarshalJAXB(rs.getString(1),
				OnPremiseSiteProviderConfig.class);
				// OnPremiseSiteProviderConfig.class.getClassLoader().getResource("onPremiseSiteProvider.xsd"));

		assertEquals(expected.getPool().size(), actual.getPool().size());

		for (int i = 0; i < expected.getPool().size(); i++) {
			PoolConfig expectedPool = expected.getPool().get(i);
			PoolConfig actualPool = actual.getPool().get(i);

			assertEquals(expectedPool.getName(), actualPool.getName());
			assertEquals(expectedPool.getAlternatePool(), actualPool.getAlternatePool());

			assertEquals(expectedPool.getSite().size(), actualPool.getSite().size());

			for (int j = 0; j < expectedPool.getSite().size(); j++) {
				Site expectedSite = expectedPool.getSite().get(j);
				Site actualSite = actualPool.getSite().get(j);

				assertEquals(expectedSite.getName(), actualSite.getName());
				assertEquals(expectedSite.getUrl(), actualSite.getUrl());
				assertEquals(expectedSite.getMaxQueries(), actualSite.getMaxQueries());
			}
		}
	}

	private PoolConfig addTestPool(OnPremiseSiteProviderConfig info, String name) {
		return addTestPool(info, name, null);
	}

	private PoolConfig addTestPool(OnPremiseSiteProviderConfig info, String name, String alternate) {
		PoolConfig pool = new PoolConfig();
		pool.setName(name);
		pool.setAlternatePool(alternate);
		info.getPool().add(pool);
		return pool;
	}

	private Site addTestSite(PoolConfig pool, String name, String url, String user, String password, int maxQueries) {
		Site site = new Site();
		site.setName(name);
		site.setUrl(url);
		site.setPassword(user);
		site.setUser(password);
		site.setMaxQueries(maxQueries);

		pool.getSite().add(site);

		return site;
	}
}
