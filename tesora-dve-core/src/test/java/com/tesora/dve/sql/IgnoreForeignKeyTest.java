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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class IgnoreForeignKeyTest extends SchemaTest {

	private static StorageGroupDDL sg = new StorageGroupDDL("check",2,"checkg");
 
	private static final ProjectDDL checkDDL =
			new PEDDL("adb",sg,
					"database").withFKMode(FKMode.IGNORE);
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	private PortalDBHelperConnectionResource conn;
	private DBHelperConnectionResource dbh;
	
	@Before
	public void before() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
		dbh = new DBHelperConnectionResource();
	}
	
	@After
	public void after() throws Throwable {
		if(conn != null) {
			conn.disconnect();
		}

		if(dbh != null) {
			dbh.disconnect();
		}

		conn = null;
		dbh = null;
	}
	
	// make sure the declaration works and we can get the info out
	@Test
	public void testDBDeclaration() throws Throwable {
		try {
			checkDDL.create(conn);
			conn.assertResults("select * from information_schema.schemata where schema_name = '" + checkDDL.getDatabaseName() + "'",
					br(nr, "def", "adb", "checkg", null, TemplateMode.OPTIONAL.toString(), "off", "ignore", "utf8", "utf8_general_ci"));
		} finally {
			checkDDL.destroy(conn);
		}
	}
	
	private static final String peinfoSql = 
			"select table_name, column_name, referenced_table_name, referenced_column_name from information_schema.key_column_usage where referenced_column_name is not null and table_schema = '" + checkDDL.getDatabaseName() + "'";
	
	private static final String nativeInfoSql =
			"select table_name, column_name, referenced_table_name, referenced_column_name from information_schema.key_column_usage where referenced_column_name is not null and table_schema = '" + sg.getPhysicalSiteNames(checkDDL.getDatabaseName()).get(0) + "'";

	// first make sure that when things are colocated we don't screw it up
	@Test
	public void testColocatedDeclaration() throws Throwable {
		try {
			checkDDL.create(conn);
			conn.execute("create range openrange (int) persistent group checkg");
			conn.execute("create table P (`id` int, `fid` int, `sid` int, primary key (`id`)) range distribute on (id) using openrange");
			conn.execute("create table C (`id` int, `fid` int, primary key (`id`), foreign key (`fid`) references P (`id`)) range distribute on (fid) using openrange");
			assertFalse("should not have warnings",conn.hasWarnings());
			Object[] results = br(nr,"C","fid","P","id"); 
			Object[] windowsNativeResults = br(nr,"c","fid","p","id");
			conn.assertResults(peinfoSql,results);
			dbh.assertResults(nativeInfoSql, (SystemUtils.IS_OS_WINDOWS ? windowsNativeResults : results));
		} finally {
			checkDDL.destroy(conn);
		}
		
	}
	
	// noncolocated - we should still report the fk in our info schema but the backing tables shouldn't have it
	@Test
	public void testNoncolocatedDeclaration() throws Throwable {
		try {
			checkDDL.create(conn);
			conn.execute("create range openrange (int) persistent group checkg");
			conn.execute("create table P (`id` int, `fid` int, `sid` int, primary key (`id`)) range distribute on (id) using openrange");
			conn.execute("create table C (`id` int, `fid` int, primary key (`id`), foreign key (`fid`) references P (`id`)) broadcast distribute");
			assertTrue("should have warnings",conn.hasWarnings());
			conn.assertResults("show warnings", 
					br(nr,"Warning",getIgnore(),
							"Invalid foreign key C.C_ibfk_1: table C is not colocated with P - not persisted"));
			conn.assertResults(peinfoSql,
					br(nr,"C","fid","P","id"));
			// make sure the fk doesn't make it onto a psite
			dbh.assertResults(nativeInfoSql, br());
		} finally {
			checkDDL.destroy(conn);
		}
	}
	
	// make sure the forward handling code still works - if it's colocated we should allow it and
	// the fk should make it to the database
	@Test
	public void testForwardColocated() throws Throwable {
		try {
			checkDDL.create(conn);
			conn.execute("set foreign_key_checks=0");
			conn.execute("create range openrange (int) persistent group checkg");
			conn.execute("create table C (`id` int, `fid` int, primary key (`id`), foreign key (`fid`) references P (`id`)) range distribute on (fid) using openrange");
			conn.execute("create table P (`id` int, `fid` int, `sid` int, primary key (`id`)) range distribute on (id) using openrange");
			assertFalse("should not have warnings",conn.hasWarnings());
			Object[] results = br(nr,"C","fid","P","id"); 
			Object[] windowsNativeResults = br(nr,"c","fid","p","id");
			conn.assertResults(peinfoSql,results);
			dbh.assertResults(nativeInfoSql, (SystemUtils.IS_OS_WINDOWS ? windowsNativeResults : results));
		} finally {
			checkDDL.destroy(conn);
		}
	}
	
	// if it's not colocated we should just silently drop it regardless of the value of foreign key checks
	@Test
	public void testForwardNonColocated() throws Throwable {
		// This test won't work on Mysql 5.1 due to a known mysql issue
		// with some FK operations
		if (SchemaTest.getDBVersion() != DBVersion.MYSQL_51) {
			try {
				checkDDL.create(conn);
				conn.execute("set foreign_key_checks=0");
				conn.execute("create range openrange (int) persistent group checkg");
				conn.execute("create table C (`id` int, `fid` int, primary key (`id`), foreign key (`fid`) references P (`id`)) broadcast distribute");
				conn.execute("create table P (`id` int, `fid` int, `sid` int, primary key (`id`)) range distribute on (id) using openrange");
				assertTrue("should have warnings",conn.hasWarnings());
				conn.assertResults("show warnings", 
						br(nr,"Warning",getIgnore(),
								"Invalid foreign key C.C_ibfk_1: table C is not colocated with P - not persisted"));
				conn.assertResults(peinfoSql,
						br(nr,"C","fid","P","id"));
				// make sure the fk doesn't make it onto a psite
				dbh.assertResults(nativeInfoSql, br());
			} finally {
				checkDDL.destroy(conn);
			}
		}
	}
	
	// if we didn't persist it, we should just noop a drop
	@Test
	public void testNonPersistedDrop() throws Throwable {
		try {
			checkDDL.create(conn);
			conn.execute("create range openrange (int) persistent group checkg");
			conn.execute("create table P (`id` int, `fid` int, `sid` int, primary key (`id`)) range distribute on (id) using openrange");
			conn.execute("create table C (`id` int, `fid` int, primary key (`id`), foreign key (`fid`) references P (`id`)) broadcast distribute");
			assertTrue("should have warnings",conn.hasWarnings());
			conn.assertResults("show warnings", 
					br(nr,"Warning",getIgnore(),
							"Invalid foreign key C.C_ibfk_1: table C is not colocated with P - not persisted"));
			conn.assertResults(peinfoSql,
					br(nr,"C","fid","P","id"));
			// make sure the fk doesn't make it onto a psite
			dbh.assertResults(nativeInfoSql, br());
			conn.execute("alter table C drop key C_ibfk_1");
			conn.assertResults(peinfoSql, br());
		} finally {
			checkDDL.destroy(conn);
		}
	}
}
