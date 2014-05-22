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
// NOPMD by doug on 04/12/12 12:05 PM

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class BalancedPersistentGroupTest extends SchemaTest {

	private static final StorageGroupDDL[] groupDDLs = { new StorageGroupDDL("group1", 1, "group1"),
			new StorageGroupDDL("group2", 1, "group2"), new StorageGroupDDL("prefix_group1", 1, "prefix_group1"),
			new StorageGroupDDL("prefix_group2", 1, "prefix_group2") };

	private static final ProjectDDL checkDDL = new PEDDL("checkdb", new StorageGroupDDL("check", 1, "checkg"), "schema");

	protected static ProxyConnectionResource conn;

	@BeforeClass
	public static void setup() throws Throwable {
		projectSetup(groupDDLs, checkDDL);
		bootHost = BootstrapHost.startServices(PETest.class);

		conn = new ProxyConnectionResource();

		checkDDL.create(conn);
		for (StorageGroupDDL group : groupDDLs) {
			group.create(conn);
		}

		DBHelper helper = buildHelper();
		try {
			helper.executeQuery("DROP DATABASE IF EXISTS group10_test1");
			helper.executeQuery("DROP DATABASE IF EXISTS group20_test2");
			helper.executeQuery("DROP DATABASE IF EXISTS prefix_group10_test3");
			helper.executeQuery("DROP DATABASE IF EXISTS prefix_group20_test4");
			helper.executeQuery("DROP DATABASE IF EXISTS prefix_group10_test5");
			helper.executeQuery("DROP DATABASE IF EXISTS prefix_group20_test6");
		} finally {
			helper.disconnect();
		}

		SchemaTest.setTemplateModeOptional();
	}

	@AfterClass
	public static void destroy() throws Throwable {
		if (conn != null) {
			conn.disconnect();
			conn = null;
		}
	}

	@Test
	public void testBasic() throws Throwable {
		conn.execute("alter dve set balance_persistent_groups=1");

		conn.execute("create database test1");
		conn.execute("create database test2");
		conn.execute("create database test3");

		// check the databases exists on the correct storage group
		conn.assertResults(
				"select default_persistent_group from information_schema.schemata where schema_name='test1'",
				br(nr, "group1"));
		conn.assertResults(
				"select default_persistent_group from information_schema.schemata where schema_name='test2'",
				br(nr, "group2"));
		conn.assertResults(
				"select default_persistent_group from information_schema.schemata where schema_name='test3'",
				br(nr, "prefix_group1"));
	}

	@Test
	public void testPrefix() throws Throwable {
		conn.execute("alter dve set balance_persistent_groups=1");
		conn.execute("alter dve set balance_persistent_groups_prefix='prefix_'");

		conn.execute("create database test4");
		conn.execute("create database test5");
		conn.execute("create database test6");

		// check the databases exists on the correct storage group
		conn.assertResults(
				"select default_persistent_group from information_schema.schemata where schema_name='test4'",
				br(nr, "prefix_group2"));
		conn.assertResults(
				"select default_persistent_group from information_schema.schemata where schema_name='test5'",
				br(nr, "prefix_group1"));
		conn.assertResults(
				"select default_persistent_group from information_schema.schemata where schema_name='test6'",
				br(nr, "prefix_group2"));
	}
}
