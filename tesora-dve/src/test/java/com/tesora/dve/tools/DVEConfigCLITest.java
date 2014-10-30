package com.tesora.dve.tools;

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

import java.util.List;

import com.tesora.dve.common.PEUrl;
import com.tesora.dve.worker.WorkerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.DynamicGroupClass;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.sql.transexec.CatalogHelper;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.variables.KnownVariables;

public class DVEConfigCLITest extends PETest {

	static final String TEST_PROVIDER = OnPremiseSiteProvider.DEFAULT_NAME;
	static final String DEFAULT_POLICY = OnPremiseSiteProvider.DEFAULT_POLICY_NAME;
	static final String TEST_POLICY = "Policy1";
	static final String LOCAL = "LOCAL";

	static String CATALOG_URL;
	static String SITE_URL;

	@BeforeClass
	public static void setup() throws Exception {
		TestCatalogHelper.createMinimalCatalog(PETest.class);
		TestHost.startServices(PETest.class);

		DVEConfigCLI pet = new DVEConfigCLI(null);
		CatalogHelper helper = pet.getCatalogHelper();

		CATALOG_URL = helper.getCatalogBaseUrl();
		SITE_URL = PEUrl.stripUrlParameters(CATALOG_URL);
	}

	@AfterClass
	public static void shutdown() {
		TestHost.stopServices();
	}

	@Test
	public void testCreateLocalCatalog() throws PEException {
		final DVEClientToolTestConsole console = new DVEClientToolTestConsole(new DVEConfigCLI(null));

		console.executeInteractiveCommand("delete catalog", "y");
		console.executeCommand("setup localhost catalog 5 5");

		// Create a CatalogDAO to verify the contents
		CatalogDAO c = CatalogDAOFactory.newInstance();

		// Verify we have the correct PersistentSites
		List<PersistentSite> sites = c.findAllPersistentSites();

		assertTrue("Should have 6 persistent sites", sites.size() == 6);

		// We expect to have 5 sites we just created
		for (int i = 1; i < sites.size(); i++) {
			verifySingleSite(c, CatalogHelper.DEFAULT_SITE_PREFIX + i, CatalogHelper.DEFAULT_SITE_PREFIX + i, CATALOG_URL);
		}

		// All of the persistent sites should be in the default persistent group
		verifyGroup(c, PEConstants.DEFAULT_GROUP_NAME, 5);

		// Verify dynamic site provider
		Provider provider = verifyProvider(c, TEST_PROVIDER, 1);

		// Verify default policy
		String defaultPolicyName = KnownVariables.DYNAMIC_POLICY.lookupPersistentConfig(c).getValue();
		assertEquals(DEFAULT_POLICY, defaultPolicyName);

		// Verify policy count
		List<DynamicPolicy> policies = c.findAllDynamicPolicies();
		assertTrue("Should have 1 dynamic policy", policies.size() == 1);

		// Verify the Dynamic Site Policy
		DynamicPolicy matchPolicy = new DynamicPolicy(DEFAULT_POLICY, true, provider.getName(), LOCAL, 1,
				provider.getName(), LOCAL, 3, provider.getName(), LOCAL, 3, provider.getName(), LOCAL, 5);

		verifyPolicy(c.findDynamicPolicy(defaultPolicyName), matchPolicy);

		c.close();
		console.close();
		console.assertValidConsoleOutput();
	}

	@Test
	public void testCreateStandardSingleCatalog() throws PEException {
		final DVEClientToolTestConsole console = new DVEClientToolTestConsole(new DVEConfigCLI(null));

		String uPersistent = PEFileUtils.getCanonicalResourcePath(this.getClass(), "single_persistent.xml");
		String uDynamic = PEFileUtils.getCanonicalResourcePath(this.getClass(), "onPremise_provider.xml");
		String uPolicy = PEFileUtils.getCanonicalResourcePath(this.getClass(), "policy1.xml");

		console.executeInteractiveCommand("delete catalog", "y");
		console.executeCommand("setup catalog '" + uPersistent + "' '" + uDynamic + "' '" + uPolicy + "'");

		// Create a CatalogDAO to verify the contents
		CatalogDAO c = CatalogDAOFactory.newInstance();

		// Verify we have the correct PersistentSites
		List<PersistentSite> sites = c.findAllPersistentSites();

		assertTrue("Should have 4 persistent sites but have " + sites.size(), sites.size() == 4);

		// We expect to have 3 sites we just created
		for (int i = 1; i < sites.size(); i++) {
			verifySingleSite(c, "site" + i, "inst" + i, SITE_URL);
		}

		// All of the persistent sites should be in the default persistent group
		verifyGroup(c, "Group1", 3);

		// Verify dynamic site provider
		Provider provider = verifyProvider(c, TEST_PROVIDER, 1);

		// Verify default policy
		String defaultPolicyName = KnownVariables.DYNAMIC_POLICY.lookupPersistentConfig(c).getValue();
		assertEquals(TEST_POLICY, defaultPolicyName);

		// Verify policy count
		List<DynamicPolicy> policies = c.findAllDynamicPolicies();
		assertTrue("Should have 1 dynamic policy", policies.size() == 1);

		// Ignore the configuration of the provider for now
		DynamicPolicy matchPolicy = new DynamicPolicy(TEST_POLICY, true, provider.getName(), "none", 1,
				provider.getName(), "none", 1, provider.getName(), "none", 2, provider.getName(), "none", 3);
		verifyPolicy(c.findDynamicPolicy(defaultPolicyName), matchPolicy);

		c.close();
		console.close();
		console.assertValidConsoleOutput();
	}

	@Test
	public void testCreateStandardMasterMasterCatalog() throws PEException {
		final DVEClientToolTestConsole console = new DVEClientToolTestConsole(new DVEConfigCLI(null));

		String uPersistent = PEFileUtils.getCanonicalResourcePath(this.getClass(), "mastermaster_persistent.xml");
		String uDynamic = PEFileUtils.getCanonicalResourcePath(this.getClass(), "onPremise_provider.xml");
		String uPolicy = PEFileUtils.getCanonicalResourcePath(this.getClass(), "policy1.xml");

		console.executeInteractiveCommand("delete catalog", "y");
		console.executeCommand("setup catalog '" + uPersistent + "' '" + uDynamic + "' '" + uPolicy + "'");

		// Create a CatalogDAO to verify the contents
		CatalogDAO c = CatalogDAOFactory.newInstance();

		// Verify we have the correct PersistentSites
		List<PersistentSite> sites = c.findAllPersistentSites();

		assertTrue("Should have 4 persistent sites but have " + sites.size(), sites.size() == 4);

		// We expect to have 3 sites we just created
		for (int i = 1; i < sites.size(); i++) {
			verifyMasterSite(c, "site" + i, "inst" + ((i * 2) - 1), "inst" + (i * 2));
		}

		// All of the persistent sites should be in the default persistent group
		verifyGroup(c, "Group1", 3);

		// Verify dynamic site provider
		Provider provider = verifyProvider(c, TEST_PROVIDER, 1);

		// Verify default policy
		String defaultPolicyName = KnownVariables.DYNAMIC_POLICY.lookupPersistentConfig(c).getValue();
		assertEquals(TEST_POLICY, defaultPolicyName);

		// Verify policy count
		List<DynamicPolicy> policies = c.findAllDynamicPolicies();
		assertTrue("Should have 1 dynamic policy", policies.size() == 1);

		DynamicPolicy matchPolicy = new DynamicPolicy(TEST_POLICY, true, provider.getName(), "none", 1,
				provider.getName(), "none", 1, provider.getName(), "none", 2, provider.getName(), "none", 3);
		verifyPolicy(c.findDynamicPolicy(defaultPolicyName), matchPolicy);

		c.close();
		console.close();
		console.assertValidConsoleOutput();
	}

	@Test
	public void testDynamicPolicy() throws PEException {
		final DVEClientToolTestConsole console = new DVEClientToolTestConsole(new DVEConfigCLI(null));
		
		String uPolicy = PEFileUtils.getCanonicalResourcePath(this.getClass(), "policy1.xml");

		console.executeInteractiveCommand("delete catalog", "y");
		console.executeCommand("setup localhost catalog 5 5");

		console.executeCommand("add policy '" + uPolicy + "'");

		// Create a CatalogDAO to verify the contents
		CatalogDAO c = CatalogDAOFactory.newInstance();

		// We should have 2 policies now
		List<DynamicPolicy> policies = c.findAllDynamicPolicies();

		assertTrue("Should have 2 dynamic policies but have " + policies.size(), policies.size() == 2);

		// the two policies should be the default one called
		// 'OnPremisePolicy" and our new one called Policy 1
		DynamicPolicy policy = c.findDynamicPolicy(DEFAULT_POLICY, false);
		assertTrue("Policy " + DEFAULT_POLICY + " should exist", policy != null);

		policy = c.findDynamicPolicy(TEST_POLICY, false);
		assertTrue("Policy " + TEST_POLICY + " should exist", policy != null);

		Provider provider = verifyProvider(c, TEST_PROVIDER, 1);

		// Check the content of our new policy
		DynamicPolicy matchPolicy = new DynamicPolicy(TEST_POLICY, true, provider.getName(), "none", 1,
				provider.getName(), "none", 1, provider.getName(), "none", 2, provider.getName(), "none", 3);
		verifyPolicy(policy, matchPolicy);
		
		c.close();

		// Set default policy test
		console.executeCommand("set default policy " + TEST_POLICY);
		
		c = CatalogDAOFactory.newInstance();

		String defaultPolicyName = KnownVariables.DYNAMIC_POLICY.lookupPersistentConfig(c).getValue();
		policy = c.findDynamicPolicy(defaultPolicyName);
		assertEquals(defaultPolicyName, TEST_POLICY);
		
		c.close();

		// NOW TEST 'configure policy'
		uPolicy = PEFileUtils.getCanonicalResourcePath(this.getClass(), "policy1b.xml");
		console.executeCommand("configure policy '" + uPolicy + "'");
		
		c = CatalogDAOFactory.newInstance();

		// We should still have 2 policies now
		policies = c.findAllDynamicPolicies();

		assertTrue("Should have 2 dynamic policies but have " + policies.size(), policies.size() == 2);

		policy = c.findDynamicPolicy(TEST_POLICY, false);
		assertTrue("Policy " + TEST_POLICY + " should exist", policy != null);

		matchPolicy = new DynamicPolicy(TEST_POLICY, false, "AnotherProvider", "test", 1,
				"AnotherProvider", "test", 2, "YetAnotherProvider", null, 3, "YetAnotherProvider", null, 5);
		// Check the content of our new policy
		verifyPolicy(policy, matchPolicy);

		c.close();
		console.close();
		console.assertValidConsoleOutput();
	}

	/*
	 * TESTS to write "set default pg <name>",
	 * "add provider <name> <plugin> [<filename>]",
	 * "configure provider <name> <filename>", "remove provider <name>",
	 * 
	 * "set groupService <Hazelcast | Localhost>"
	 * 
	 * "add es <name> <plugin> <usesDataStore> [<connectUser>] [<filename>]",
	 * "configure es <name> <filename>", "remove es <name>",
	 */

	private void verifySingleSite(CatalogDAO c, String siteName, String instName, String siteUrl) throws PEException {
		PersistentSite site = c.findPersistentSite(siteName);

		assertTrue("Site " + siteName + " should exist", site != null);
		assertEquals(WorkerFactory.SINGLE_DIRECT_HA_TYPE, site.getHAType());

		// Verify the site instance
		List<SiteInstance> instances = site.getSiteInstances();
		assertTrue("Should have 1 site instance", instances.size() == 1);
		SiteInstance inst = instances.get(0);
		assertEquals(instName, inst.getName());
		String url = inst.getInstanceURL();
		assertEquals(siteUrl, url);
		assertTrue("Site should be enabled", inst.isEnabled() == true);
		assertTrue("Site should be master", inst.isMaster() == true);

		// and check that the site is using the same information as the site
		// instance
		assertEquals(inst, site.getMasterInstance());
		assertEquals(url, site.getMasterUrl());
	}

	private void verifyMasterSite(CatalogDAO c, String siteName, String inst1Name, String inst2Name) throws PEException {
		PersistentSite site = c.findPersistentSite(siteName);

		assertTrue("Site " + siteName + " should exist", site != null);
		assertEquals(WorkerFactory.MASTER_MASTER_HA_TYPE, site.getHAType());

		// Verify the site instance
		List<SiteInstance> instances = site.getSiteInstances();
		assertTrue("Should have 2 site instance", instances.size() == 2);

		SiteInstance inst1 = instances.get(0);
		assertEquals(inst1Name, inst1.getName());
		String url1 = inst1.getInstanceURL();
		assertEquals(SITE_URL, url1);
		assertTrue("Site should be enabled", inst1.isEnabled() == true);
		assertTrue("Site should be master", inst1.isMaster() == true);

		SiteInstance inst2 = instances.get(1);
		assertEquals(inst2Name, inst2.getName());
		String url2 = inst2.getInstanceURL();
		assertEquals(SITE_URL, url2);
		assertTrue("Site should be enabled", inst2.isEnabled() == true);
		assertTrue("Site should be master", inst2.isMaster() == false);

		// and check that the site is using the same information as the site
		// instance
		assertEquals(inst1, site.getMasterInstance());
		assertEquals(url1, site.getMasterUrl());
	}

	private void verifyGroup(CatalogDAO c, String name, int count) throws PEException {
		// All of the persistent sites should be in the default persistent group
		String defSGName = KnownVariables.PERSISTENT_GROUP.lookupPersistentConfig(c).getValue();
		assertEquals(name, defSGName);

		PersistentGroup group = c.findPersistentGroup(defSGName);
		assertEquals(name, group.getName());

		List<PersistentSite> groupSites = group.getStorageSites();
		assertTrue("Should have " + count + " persistent sites in default group", groupSites.size() == count);
	}

	private Provider verifyProvider(CatalogDAO c, String name, int count) throws PEException {
		// Verify dynamic site provider
		List<Provider> providers = c.findAllProviders();
		assertTrue("Should have " + count + " provider", providers.size() == count);

		Provider provider = c.findProvider(name);
		assertEquals(name, provider.getName());
		assertEquals(OnPremiseSiteProvider.class.getCanonicalName(), provider.getPlugin());

		return provider;
	}

	private void verifyPolicy(DynamicPolicy policy, DynamicPolicy match) {
		assertEquals(match.getName(), policy.getName());
		assertEquals(match.getStrict(), policy.getStrict());

		verifyDynamicGroupClass(policy.getAggregationClass(), match.getAggregationClass());
		verifyDynamicGroupClass(policy.getSmallClass(), match.getSmallClass());
		verifyDynamicGroupClass(policy.getMediumClass(), match.getMediumClass());
	}
	
	private void verifyDynamicGroupClass(DynamicGroupClass cls, DynamicGroupClass matchCls) {
		assertEquals(matchCls.getProvider(), cls.getProvider());
		assertEquals(matchCls.getCount(), cls.getCount());
		assertEquals(matchCls.getPoolName(), cls.getPoolName());
	}
}