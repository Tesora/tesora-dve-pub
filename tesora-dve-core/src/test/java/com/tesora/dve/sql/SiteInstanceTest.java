package com.tesora.dve.sql;

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

import static org.junit.Assert.assertTrue;

import com.tesora.dve.worker.WorkerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PECryptoUtils;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class SiteInstanceTest extends SchemaTest {

	private static final ProjectDDL checkDDL = new PEDDL("checkdb",
			new StorageGroupDDL("check", 1, "checkg"), "schema");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	protected ProxyConnectionResource conn;
	protected DBHelperConnectionResource dbh;

	protected String catalogUser;
	protected String catalogPass;
	protected String encryptedCatalogPass;
	
	@Before
	public void connect() throws Throwable {
		conn = new ProxyConnectionResource();
		checkDDL.create(conn);
		dbh = new DBHelperConnectionResource();
		catalogUser = TestCatalogHelper.getInstance().getCatalogUser();
		catalogPass = TestCatalogHelper.getInstance().getCatalogPassword();
		encryptedCatalogPass = PECryptoUtils.encrypt(catalogPass);
	}

	@After
	public void disconnect() throws Throwable {
		if(conn != null)
			conn.disconnect();
		conn = null;
		if(dbh != null)
			dbh.disconnect();
		dbh = null;
	}
	
	private String getCreds() throws Throwable {
		return "user='" + catalogUser + "' password='" + catalogPass + "'";
	}
	
	@Test
	public void test() throws Throwable {
		conn.execute("create persistent instance si1 url='test.url1' status='online' " + getCreds());
		conn.execute("create persistent instance si2 url='test.url2' status='online' " + getCreds());
		
		conn.assertResults("show persistent instances like 'si%'",
				br(nr, "si1", null, "test.url1", catalogUser, encryptedCatalogPass, "NO", "ONLINE",
				   nr, "si2", null, "test.url2", catalogUser, encryptedCatalogPass, "NO", "ONLINE"));
		
		// make sure old syntax for creating persistent site works
		conn.execute("create persistent site ss1 url='myurl' " + getCreds());
		conn.assertResults("show persistent site ss1",
				br(nr, "ss1", WorkerFactory.SINGLE_DIRECT_HA_TYPE, "myurl"));

		// drop ss1
		conn.execute("drop persistent site ss1");
		conn.assertResults("show persistent sites like 'ss1'", br());

		// make sure new single site syntax for creating persistent site works
		conn.execute("create persistent site ss1 url='myurl' " + getCreds());
		conn.assertResults("show persistent site ss1",
				br(nr, "ss1", WorkerFactory.SINGLE_DIRECT_HA_TYPE, "myurl"));

		// add existing persistent instance when creating new persistent site
		conn.execute("create persistent site ss2 of type MasterMaster set master si1");
		conn.assertResults("show persistent sites like 'ss2'",
				br(nr, "ss2", WorkerFactory.MASTER_MASTER_HA_TYPE, "test.url1"));

		// add the persistent instances to the new persistent site
		conn.execute("alter persistent site ss2 add si2");
		conn.assertResults("show persistent sites like 'ss2'",
				br(nr, "ss2", WorkerFactory.MASTER_MASTER_HA_TYPE, "test.url1"));
		conn.assertResults("show persistent instances like 'si%'",
				br(nr, "si1", "ss2", "test.url1", catalogUser, encryptedCatalogPass, "YES", "ONLINE",
				   nr, "si2", "ss2", "test.url2", catalogUser, encryptedCatalogPass, "NO", "ONLINE"));

		// change the persistent instance info 
		conn.execute("alter persistent instance si1 URL='new si1 url' STATUS='OFFLINE'");
		conn.execute("alter persistent instance si2 URL='new si2 url'");
		conn.assertResults("show persistent instances like 'si%'",
				br(nr, "si1", "ss2", "new si1 url", catalogUser, encryptedCatalogPass, "YES", "OFFLINE",
				   nr, "si2", "ss2", "new si2 url", catalogUser, encryptedCatalogPass, "NO", "ONLINE"));
		
		// remove si1 from the persistent site 
		conn.execute("alter persistent site ss2 drop si1");
		conn.assertResults("show persistent sites like 'ss2'",
				br(nr, "ss2", WorkerFactory.MASTER_MASTER_HA_TYPE, "new si2 url"));
		conn.assertResults("show persistent instances like 'si%'",
				br(nr, "si1", null, "new si1 url", catalogUser, encryptedCatalogPass, "YES", "OFFLINE",
				   nr, "si2", "ss2", "new si2 url", catalogUser, encryptedCatalogPass, "YES", "ONLINE"));

		// drop ss2
		conn.execute("drop persistent site ss2");
		conn.assertResults("show persistent sites like 'ss2'", br());
		
		// only si2 was associated with ss2 make sure it is gone but si1 remains
		conn.assertResults("show persistent instances like 'si1'",
				br(nr, "si1", null, "new si1 url", catalogUser, encryptedCatalogPass, "YES", "OFFLINE"));
		conn.assertResults("show persistent instances like 'si2'",
				br());
	
		conn.execute("drop persistent instance si1");
		conn.assertResults("show persistent instances like 'si1'",
				br());
		
		// create a new persistent site then add the persistent instances
		conn.execute("create persistent instance si3 url='test.url3' status='online' " + getCreds());
		conn.execute("create persistent instance si4 url='test.url4' status='offline' " + getCreds());
		conn.assertResults("show persistent instances like 'si%'",
				br(nr, "si3", null, "test.url3", catalogUser, encryptedCatalogPass, "NO", "ONLINE",
				   nr, "si4", null, "test.url4", catalogUser, encryptedCatalogPass, "NO", "OFFLINE"));

		conn.execute("create persistent site ss3 of type MasterMaster set master si3 add si4");
		conn.assertResults("show persistent sites like 'ss3'",
				br(nr, "ss3", WorkerFactory.MASTER_MASTER_HA_TYPE, "test.url3"));
		conn.assertResults("show persistent instances like 'si%'",
				br(nr, "si3", "ss3", "test.url3", catalogUser, encryptedCatalogPass, "YES", "ONLINE",
				   nr, "si4", "ss3", "test.url4", catalogUser, encryptedCatalogPass, "NO", "OFFLINE"));

		// change HA type 
		conn.execute("alter persistent site ss3 set type single");
		conn.assertResults("show persistent sites like 'ss3'",
				br(nr, "ss3", WorkerFactory.SINGLE_DIRECT_HA_TYPE, "test.url3"));

		// change master sites 
		conn.execute("alter persistent instance si4 status='online'");
		conn.execute("alter persistent site ss3 set master si4");
		conn.assertResults("show persistent sites like 'ss3'",
				br(nr, "ss3", WorkerFactory.SINGLE_DIRECT_HA_TYPE, "test.url4"));
		conn.assertResults("show persistent instances like 'si%'",
				br(nr, "si3", "ss3", "test.url3", catalogUser, encryptedCatalogPass, "NO", "ONLINE",
				   nr, "si4", "ss3", "test.url4", catalogUser, encryptedCatalogPass, "YES", "ONLINE"));
	}

	@Test
	public void testSiteInstanceParserErrors() throws Throwable {
		// test not specifying mandatory parameters generates parser exceptions
		
		String query = "create persistent instance";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
	
		query = "create persistent instance url='test.url1', status='online', master='true'";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));

		query = "create persistent instance failedinstance url='test.url1', enabled='true'";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));

		query = "alter persistent instance";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));

		query = "alter persistent instance url='test.url1', status='online', master='true'";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
		
		query = "alter persistent instance url='test.url1', status='failure'";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));

		query = "drop persistent instance";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
		
		query = "create persistent site of type MasterMaster";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));

		query = "create persistent instance errorsi1 url='test.url1', status='failed'";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
	}
	
	@Test
	public void testStorageSiteParserErrors() throws Throwable {
		// test not specifying mandatory parameters generates parser exceptions
		
		String query = "create persistent site";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
		
		query = "create persistent site OF";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
		
		query = "create persistent site ss OF TYPE";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
		
		conn.execute("create persistent instance errsi3 url='test.url3' status='online' " + getCreds());
		query = "create persistent site foo of type something set master errsi3";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
		
		query = "create persistent site foo of type mastermaster set master";
		assertTrue("Expected parsing failure on sql: " + query, 
				executeExpectedFailure(query));
	}
	
	@Test
	public void testDuplicateErrors() throws Throwable {
		
		String query = "create persistent instance myuniquesi url='test.url1' " + getCreds();
		conn.execute(query);
		assertTrue("Expected duplicate failure on sql: " + query, 
				executeExpectedFailure(query));

		query = "create persistent site ss OF TYPE single set master myuniquesi";
		conn.execute(query);
		assertTrue("Expected duplicate error on sql: " + query, 
				executeExpectedFailure(query));
	}
	
	@Test
	public void testCannotAddExistingSiteInstanceErrors() throws Throwable {
		
		// create a persistent site with a persistent instance
		String query = "create persistent instance dupsi url='test.url1' " + getCreds();
		conn.execute(query);

		query = "create persistent site dupss OF TYPE MasterMaster set master dupsi";
		conn.execute(query);
		
		query = "create persistent site anotherss OF TYPE MasterMaster set master dupsi";
		assertTrue("Expected cannot reuse persistent instance on sql: " + query, 
				executeExpectedFailure(query));
	}

	boolean executeExpectedFailure(String query) throws Throwable {
		boolean testFailed = true;
		try {
			conn.execute(query);
			testFailed = false;
		} catch (SchemaException e) {
			// expected
		} catch (PEException e) {
			// expected
		}
		return testFailed;
	}
}
