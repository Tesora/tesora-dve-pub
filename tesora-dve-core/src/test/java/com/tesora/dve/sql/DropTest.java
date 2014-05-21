// OS_STATUS: public
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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;

// specifically for testing drop range, persistent group, persistent site
public class DropTest extends SchemaTest {

	private static final int SITES = 5;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,2,"sysg"),
				"schema");
	private static final StorageGroupDDL sysGroup =
		new StorageGroupDDL("esys",SITES,2,"esysg");

	// so, the structure of this test is -
	// create two ranges, a couple of databases, a few tables, and at least two generations
	// then try to clean it all up.  the drop order is
	// drop databases
	// drop ranges
	// drop persistent groups
	// drop persistent sites
	// we should make sure we fail appropriately if any persistent data would be lost at each drop
	// one more thing we have to do is repeat the creates at the end, make sure everything shows up
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(new StorageGroupDDL[] { sysGroup}, sysDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource popconn = new ProxyConnectionResource();
		removeUser(popconn,userName,userAccess);
		popconn.execute("create user '" + userName + "'@'" + userAccess + "' identified by '" + userName + "'");
		popconn.disconnect();
	}

	private static final String userName = "regular";
	private static final String userAccess = "localhost";
	
	protected ProxyConnectionResource conn;	
	protected TestResource resource;
	
	@Before
	public void connect() throws Throwable {
		conn = new ProxyConnectionResource();
		resource = new TestResource(conn, sysDDL);
	}

	@After
	public void disconnect() throws Throwable {
		conn.disconnect();
		conn = null;
		resource = null;
	}
	
	private static List<MirrorTest> buildPopulate() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		// first create the db specific ranges
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				ConnectionResource conn = mr.getConnection();
				String dbname = mr.getDDL().getDatabaseName();
				String simpleRange = "s" + dbname;
				String complexRange = "c" + dbname;
				conn.execute("create range " + simpleRange + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				conn.execute("create range " + complexRange + " (int, int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				return null;
			}
		});
		// now create 3 tables per db - one that uses simple, one that uses complex, and one that uses neither
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				ConnectionResource conn = mr.getConnection();
				String dbname = mr.getDDL().getDatabaseName();
				String simpleRange = "s" + dbname;
				String complexRange = "c" + dbname;
				conn.execute("create table ctab (`aid` int unsigned, `bid` int unsigned, `a` int, `b` int, primary key (`aid`,`bid`)) range distribute on (`aid`, `bid`) using " + complexRange);
				conn.execute("create table stab (`aid` int unsigned, `bid` int unsigned, `a` int, `b` int, primary key (`aid`)) range distribute on (`aid`) using " + simpleRange);
				conn.execute("create table ntab (`aid` int unsigned, `bid` int unsigned, `a` int, `b` int, primary key (`aid`))");
				return null;
			}
			
		});
		// populate the tables - do 10 rows before we add the gen, 10 rows after
		// the row values will be specified only - we'll fill in the columns later
		ArrayList<String> values = new ArrayList<String>();
		for(int i = 0; i < 20; i++) {
			values.add("('" + i + "','" + 2*i + "','" + (i + 1) + "','" + 2*(i + 1) + "')");
		}
		List<String> firstHalf = values.subList(0, 9);
		List<String> secondHalf = values.subList(9, 19);
		String[] tabs = new String[] { "ctab", "stab", "ntab" };
		for(int i = 0; i < tabs.length; i++)
			out.add(new StatementMirrorProc("insert into " + tabs[i] + " (`aid`, `bid`, `a`, `b`) values " + Functional.join(firstHalf, ", ")));
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				String gens = mr.getDDL().getPersistentGroup().getAddGenerations();
				ConnectionResource conn = mr.getConnection();
				if (gens != null) {
					conn.disconnect();
					conn.connect();
					conn.execute("use " + mr.getDDL().getDatabaseName());
					// execute any holdbacks
					conn.execute(gens);
				}
				return null;
			}
			
		});
		for(int i = 0; i < tabs.length; i++)
			out.add(new StatementMirrorProc("insert into " + tabs[i] + " (`aid`, `bid`, `a`, `b`) values " + Functional.join(secondHalf, ", ")));
		return out;
	}
	
	// we're actually going to run the whole test a few times just to make sure
	@Test
	public void testAll() throws Throwable {
		TestResource poptr = new TestResource(conn, sysDDL);
		List<MirrorTest> pop = buildPopulate();
		for(int i = 0; i < 3; i++) {
			sysDDL.clearCreated();
			sysDDL.create(conn);
			for(MirrorTest mt : pop)
				mt.execute(poptr, null);
			testParts();
		}
	}
	
	private void testParts() throws Throwable {
		testPermissions();
		testDropsDataExisting();
		testDrops();
	}
	
	private void testPermissions() throws Throwable {
		try (ProxyConnectionResource userConn = new ProxyConnectionResource(userName, userName)) {
			// first off, let's make sure the nonroot user can't delete any of these things
			try {
				userConn.execute("drop range csysdb");
			} catch (PEException e) {
				assertSchemaException(e,"You do not have permission to drop a range");
			}
			try {
				userConn.execute("drop persistent group sysg");
			} catch (PEException e) {
				assertSchemaException(e,"You do not have permission to drop a persistent group");
			}
			try {
				userConn.execute("drop persistent site sys1");
			} catch (PEException e) {
				assertSchemaException(e,"You do not have permission to drop a persistent site");
			}
		}
	}

	private void testDropsDataExisting() throws Throwable {
		try {
			conn.execute("drop range csysdb");
		} catch (PEException e) {
			assertSchemaException(e,"Unable to drop range csysdb because used by table ctab");
		}
		try {
			conn.execute("drop persistent group sysg");
		} catch (PEException e) {
			assertSchemaException(e,"Unable to drop persistent group sysg because used by database sysdb");
		}
		try {
			conn.execute("drop persistent site sys1");
		} catch (PEException e) {
			assertSchemaException(e,"Unable to drop persistent site sys1 because used by group sysg");
		}
	}
	
	private void testDrops() throws Throwable {
		conn.execute("drop table stab");
		conn.assertResults("show tables like 'stab'",br());
		conn.execute("drop range ssysdb");
		conn.assertResults("show ranges like 'ssysdb'",br());
		try {
			conn.execute("drop range csysdb");
		} catch (PEException e) {
			assertSchemaException(e,"Unable to drop range csysdb because used by table ctab");
		}
		conn.execute("drop table ctab");
		conn.assertResults("show tables like 'ctab'", br());
		conn.execute("drop range csysdb");
		conn.assertResults("show ranges",br());
		try {
			conn.execute("drop persistent group sysg");
		} catch (PEException e) {
			assertSchemaException(e,"Unable to drop persistent group sysg because used by database sysdb");
		}
		conn.execute("drop database sysdb");
		try {
			conn.execute("drop persistent site sys4");
		} catch (PEException e) {
			assertSchemaException(e,"Unable to drop persistent site sys4 because used by group sysg");
		}
		conn.execute("drop persistent group sysg");
		conn.assertResults("show persistent groups like 'sysg'",br());
		for(int i = 0; i < SITES; i++) {
			String ssname = "sys" + i;
			conn.execute("drop persistent site " + ssname);
			conn.assertResults("show persistent sites like '" + ssname + "'",br());
		}		
	}	
	
	@Test
	public void testCaseSensitiveDrop() throws Throwable {
		// execute only if lower_case_table_names is > 0
		NativeDDL nativeDDL = new NativeDDL("checkdb");
		DBHelperConnectionResource nativeConnection = null;
		try {
			nativeConnection = new DBHelperConnectionResource();
			TestResource nativeResource = new TestResource(nativeConnection, nativeDDL);
			ResourceResponse rr = nativeResource.getConnection().execute("SHOW VARIABLES LIKE 'lower_case_table_names'");
			Integer value = Integer.valueOf((String)(rr.getResults().get(0).getRow().get(1).getColumnValue()));
			if (value > 0) {
				// test only valid for OSes that are case sensitive
				// ie. this test does not run for windows and mac
				return;
			}
		} finally {
			if (nativeConnection != null) {
				nativeConnection.disconnect();
			}
		}

		final int LOWER_INDEX  = 0;
		final int UPPER_INDEX  = 1;
		String[] names = {"etun", "eTun"};
		String[] creates = {"create table if not exists `etun` (`id` int unsigned)",
				"create table if not exists `eTun` (`id` int unsigned)"};

		try {
			sysDDL.clearCreated();
			sysDDL.create(resource);
			
			conn.execute("use " + resource.getDDL().getDatabaseName());
			
			// unquoted lower identifier lower and upper tables
			conn.execute(creates[LOWER_INDEX]);
			conn.execute(creates[UPPER_INDEX]);
			conn.execute("DROP TABLE " + names[LOWER_INDEX]);
			conn.assertResults("SHOW TABLES", br(nr,names[UPPER_INDEX]));
	
			// unquoted upper identifier lower and upper tables
			conn.execute(creates[LOWER_INDEX]);
			conn.execute(creates[UPPER_INDEX]);
			conn.execute("DROP TABLE " + names[UPPER_INDEX]);
			conn.assertResults("SHOW TABLES", br(nr,names[LOWER_INDEX]));
	
			// quoted lower identifier lower and upper tables
			conn.execute(creates[LOWER_INDEX]);
			conn.execute(creates[UPPER_INDEX]);
			conn.execute("DROP TABLE " + quotedIdentifier(names[LOWER_INDEX]));
			conn.assertResults("SHOW TABLES", br(nr,names[UPPER_INDEX]));
	
			// quoted upper identifier lower and upper tables
			conn.execute(creates[LOWER_INDEX]);
			conn.execute(creates[UPPER_INDEX]);
			conn.execute("DROP TABLE " + quotedIdentifier(names[UPPER_INDEX]));
			conn.assertResults("SHOW TABLES", br(nr,names[LOWER_INDEX]));
	
			conn.execute("DROP TABLE IF EXISTS " + quotedIdentifier(names[LOWER_INDEX]));
			conn.execute("DROP TABLE IF EXISTS " + quotedIdentifier(names[UPPER_INDEX]));
	
			// unquoted lower identifier lower table
			conn.execute(creates[LOWER_INDEX]);
			conn.execute("DROP TABLE " + names[LOWER_INDEX]);
			conn.assertResults("SHOW TABLES", br());
	
			// unquoted upper identifier lower table
			conn.execute(creates[LOWER_INDEX]);
			conn.execute("DROP TABLE " + names[UPPER_INDEX]);
			conn.assertResults("SHOW TABLES", br());
	
			// quoted lower identifier lower table
			conn.execute(creates[LOWER_INDEX]);
			conn.execute("DROP TABLE " + quotedIdentifier(names[LOWER_INDEX]));
			conn.assertResults("SHOW TABLES", br());
	
			// quoted upper identifier lower table
			conn.execute(creates[LOWER_INDEX]);
			try {
				conn.execute("DROP TABLE " + quotedIdentifier(names[UPPER_INDEX]));
				fail("expected drop of quoted identifer to be not found");
			} catch (Exception e) {
				// test worked
			}
	
			conn.execute("DROP TABLE IF EXISTS " + quotedIdentifier(names[LOWER_INDEX]));
			conn.execute("DROP TABLE IF EXISTS " + quotedIdentifier(names[UPPER_INDEX]));
	
			// unquoted lower identifier upper table
			conn.execute(creates[UPPER_INDEX]);
			conn.execute("DROP TABLE " + names[LOWER_INDEX]);
			conn.assertResults("SHOW TABLES", br());
	
			// unquoted upper identifier upper table
			conn.execute(creates[UPPER_INDEX]);
			conn.execute("DROP TABLE " + names[UPPER_INDEX]);
			conn.assertResults("SHOW TABLES", br());
	
			// quoted lower identifier upper table
			conn.execute(creates[UPPER_INDEX]);
			try {
				conn.execute("DROP TABLE " + quotedIdentifier(names[LOWER_INDEX]));
				fail("expected drop of quoted identifer to be not found");
			} catch (Exception e) {
				// test worked
			}
	
			// quoted upper identifier upper table
			conn.execute(creates[UPPER_INDEX]);
			conn.execute("DROP TABLE " + quotedIdentifier(names[UPPER_INDEX]));
			conn.assertResults("SHOW TABLES", br());
		} finally {
			sysDDL.destroy(conn);
			conn.disconnect();
		}
	}
	
	private String quotedIdentifier(String name) {
		return "`" + name + "`";
	}

	@Test
	public void testDropRange() throws Throwable {
		try {
			sysDDL.clearCreated();
			sysDDL.create(resource);
			
			conn.execute("drop range if exists non_existent_range");
			
			try {
				conn.execute("drop range non_existent_range");
				fail("DROP RANGE non-existent range should throw exceptions");
			} catch (Exception e) {
				// expected
			}

			conn.execute("create range existent_range (int) persistent group " + sysDDL.getPersistentGroup().getName());
			conn.execute("drop range existent_range");

			conn.execute("create range existent_range (int) persistent group " + sysDDL.getPersistentGroup().getName());
			conn.execute("drop range if exists existent_range");
		} finally {
			sysDDL.destroy(conn);
			conn.disconnect();
		}
	}
	
	@Test
	public void testPE285() throws Throwable {
		try {
			sysDDL.clearCreated();
			sysDDL.create(conn);
			conn.execute("create table pe285 (id int, sid int, index (sid))");
			conn.execute("drop index sid on pe285");
			conn.assertResults("show keys in pe285", br());
		} finally {
			sysDDL.destroy(conn);
			conn.disconnect();
		}
	}
	
	@Test
	public void testPE206() throws Throwable {
		try {
			sysDDL.clearCreated();
			sysDDL.create(conn);
			conn.execute("create table pe206a (id int)");
			conn.execute("create table pe206c (id int)");
			conn.execute("create table pe206e (id int)");
			conn.execute("create table pe206g (id int)");
			conn.assertResults("show tables like 'pe206%'", br(nr,"pe206a", nr, "pe206c", nr, "pe206e", nr, "pe206g"));
			try {
				conn.execute("drop table pe206a, pe206b, pe206c, pe206d");
			} catch (PEException e) {
				assertEquals("Unknown table 'pe206b,pe206d'", e.getMessage());
			}
			// even though there was an exception the existing tables should be deleted
			conn.assertResults("show tables like 'pe206%'", br(nr, "pe206e", nr, "pe206g"));
			// the if exists should cause the unknown table to not throw an exception
			conn.execute("drop table if exists pe206d, pe206e, pe206f");
			// TODO check for warnings
			conn.assertResults("show tables like 'pe206%'", br(nr, "pe206g"));

			conn.execute("drop table if exists knownNotExists");
			try {
				conn.execute("drop table knownNotExists");
			} catch (PEException e) {
				assertEquals("Unable to build plan - No such table(s) 'knownNotExists'", e.getMessage());
			}
			try {
				conn.execute("drop table knownNotExists1, knownNotExists2");
			} catch (PEException e) {
				assertEquals("Unable to build plan - No such table(s) 'knownNotExists1,knownNotExists2'", e.getMessage());
			}

			// make sure the storage sites actually were dropped so recreate same tables
			conn.execute("create table pe206a (id int)");
			conn.execute("create table pe206c (id int)");
			conn.execute("create table pe206e (id int)");
			conn.assertResults("show tables like 'pe206%'", br(nr,"pe206a", nr, "pe206c", nr, "pe206e", nr, "pe206g"));
		} finally {
			sysDDL.destroy(conn);
			conn.disconnect();
		}
	}
}
