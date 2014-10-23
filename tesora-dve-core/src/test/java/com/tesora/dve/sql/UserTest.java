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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;

public class UserTest extends SchemaTest {

	private static final ProjectDDL testDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",3,"checkg"),
				"schema");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(testDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	protected PortalDBHelperConnectionResource conn;
	protected DBHelperConnectionResource dbh;
	
	@Before
	public void before() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
		testDDL.create(conn);
		dbh = new DBHelperConnectionResource();		
	}
	
	@After
	public void after() throws Throwable {
		if(conn != null)
			conn.disconnect();
		conn = null;
		if(dbh != null)
			dbh.disconnect();
		dbh = null;
		CatalogDAOFactory.clearCache();
	}

	private void removeUser() throws Throwable {
		String[] host = new String[] { "localhost", "127.0.0.1", "%" }; // NOPMD by doug on 04/12/12 12:06 PM
		for(int i = 0; i < host.length; i++)
			removeUser(conn,"dguser",host[i]);
	}
	
	@Test
	public void testCreate() throws Throwable {
		TestResource tr = new TestResource(conn,testDDL);
		String host = "localhost";
		removeUser();
		conn.execute("create user 'dguser'@'" + host + "' identified by 'dguser'");
		dbh.assertResults("select User, Host from mysql.user where User like '%dguser%'", 
				br(nr,"dguser",host));
		ResourceResponse resp = dbh.fetch("select Password from mysql.user where User like '%dguser%'");
		Object oldPassword = resp.getResults().get(0).getResultColumn(1).getColumnValue();
		conn.execute("set password for 'dguser'@'" + host + "' = PASSWORD('doh!')");
		resp = dbh.fetch("select Password from mysql.user where User like '%dguser%'");
		Object newPassword = resp.getResults().get(0).getResultColumn(1).getColumnValue();
		assertTrue(!oldPassword.equals(newPassword));
		// make sure the catalog matches
		SchemaContext sc = tr.getContext();
		PEUser u = sc.findUser("dguser",host);
		assertEquals(u.getPassword(),"doh!");
	}
	
	@Test
	public void testCreateViaGrant() throws Throwable {
		TestResource tr = new TestResource(conn,testDDL);
		String[] scoping = new String[] { "checkdb.*", "*.*" };
		// we're only going to drop the first time - part of this test is to make sure that repeated grants work
		String host = "localhost";
		removeUser();
		conn.disconnect();
		conn.connect();
		for(int i = 0; i < scoping.length; i++) {
			// grant is no longer propagated, must make sure that user is still propagated
			conn.execute("grant all on " + scoping[i] + " to 'dguser'@'" + host + "' identified by 'dguser'");
			dbh.assertResults("select User, Host from mysql.user where User like '%dguser%'", 
					br(nr,"dguser",host));
			// make sure the catalog matches
			SchemaContext sc = tr.getContext();
			PEUser u = sc.findUser("dguser",host);
			assertNotNull("user must exist",u);
			// make sure flush privileges works
			conn.execute("flush privileges");
		}
	}
	
	@Test
	public void testMultipleGrant() throws Throwable {
		removeUser();
		conn.disconnect();
		conn.connect();
		conn.execute("grant all on checkdb.* to 'dguser'@'localhost' identified by 'dguser'");
		conn.execute("grant all on checkdb.* to 'dguser'@'127.0.0.1' identified by 'dguser'");
		conn.execute("grant all on checkdb.* to 'dguser'@'%' identified by 'dguser'");
		ProxyConnectionResource other = null;
		try {
			other = new ProxyConnectionResource("dguser","dguser");
			other.execute("use " + testDDL.getDatabaseName());
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			other.disconnect();
		}		
	}
	
	@Test
	public void testDrop() throws Throwable {
		String host = "localhost";
		removeUser();
		conn.execute("create user 'dguser'@'" + host + "' identified by 'dguser'");
		dbh.assertResults("select User, Host from mysql.user where User like '%dguser%'", 
				br(nr,"dguser",host));
		conn.execute("drop user 'dguser'@'" + host + "'");
		dbh.assertResults("select User, Host from mysql.user where User like '%dguser%'",br());
		conn.execute("drop user if exists 'dguser'@'" + host + "'");
	}

	@Test 
	public void testGrantAfterCreate() throws Throwable {
		String host = "localhost";
		removeUser();
		// specifically - create the user before there's even a persistent group
		testDDL.destroy(conn);
		testDDL.clearCreated();
		// specifically - create the user before the db, then add a grant afterwords
		conn.execute("create user 'dguser'@'" + host + "' identified by 'dguser'");
		testDDL.create(conn);
		conn.execute("grant all on " + testDDL.getDatabaseName() + ".* to 'dguser'@'" + host + "'");
		
		try (ProxyConnectionResource other = new ProxyConnectionResource("dguser", "dguser")) {
			other.execute("use " + testDDL.getDatabaseName());
			other.execute("create table `ctest` (`id` int) random distribute");
			other.execute("insert into `ctest` values (1), (2), (3)");
			other.assertResults("select id from ctest order by id", br(nr,new Integer(1),nr,new Integer(2),nr,new Integer(3)));
		}
		
	}	

	@Test
	public void testPE_379_380() throws Throwable {
		String host = "localhost";
		
		String[] quotes = {"'", "\"", "`", ""};
		Boolean[] hasScopeOptions = {true, false};
		
		String user = "USER_PE380";
		// test the different quoted/unquoted and scope/no scope combos
		for(String quote : quotes) {
			StringBuilder user_part = new StringBuilder();
			user_part.append(quote);
			user_part.append(user);
			user_part.append(quote);

			for(Boolean hasScope : hasScopeOptions) {
				for(String scopeQuote : quotes) {
					StringBuilder scopePart = new StringBuilder();
					if (hasScope) {
						scopePart.append("@");
						scopePart.append(scopeQuote);
						scopePart.append(host);
						scopePart.append(scopeQuote);
					}
					
					// build this string for the drop
					String userId = user_part.toString() + scopePart.toString();
					removeUser(conn,user,host);
					// put it all together and execute the query 
					StringBuilder query = new StringBuilder();
					query.append("CREATE USER ");
					query.append(userId);
					query.append(" IDENTIFIED BY 'password'");
					
					conn.execute(query.toString());
					dbh.assertResults("select User, Host from mysql.user where User = '" + user + "'", 
							br(nr,user,hasScope ? host : "%"));
					
					// sanity test the grant works for PE-379
					conn.execute("GRANT ALL ON *.* TO " + userId + " IDENTIFIED BY 'password'");
					
					conn.execute("DROP USER " + userId);
					dbh.assertResults("select User, Host from mysql.user where User = '" + user + "'", 
							br());
				}
			}
		}
	}
}
