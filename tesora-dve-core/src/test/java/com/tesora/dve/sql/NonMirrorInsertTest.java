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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class NonMirrorInsertTest extends SchemaTest {

	private static final ProjectDDL checkDDL = new PEDDL("checkdb", new StorageGroupDDL("check", 2, "checkg"), "schema");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	protected ProxyConnectionResource conn;

	@Before
	public void connect() throws Throwable {
		conn = new ProxyConnectionResource();
		checkDDL.create(conn);
	}

	@After
	public void disconnect() throws Throwable {
		if (conn != null) {
			conn.disconnect();
		}
		conn = null;
	}

	@Test
	public void testPE1567() throws Throwable {
		final String dbName = checkDDL.getDatabaseName();
		final String rangeName = "pe1567_range";
		final QualifiedName tableName = new QualifiedName(new UnqualifiedName(dbName), new UnqualifiedName("pe1567"));

		conn.execute("DROP TABLE IF EXISTS " + tableName.getSQL());
		conn.execute("DROP RANGE IF EXISTS " + rangeName);
		conn.execute("CREATE RANGE " + rangeName + " (int) PERSISTENT GROUP " + checkDDL.getPersistentGroup().getName());
		conn.execute("CREATE TABLE " + tableName.getSQL() + " (id INT, value INT) /*#dve range distribute on (`id`) using " + rangeName + " */");
		
		conn.execute("USE INFORMATION_SCHEMA");
		conn.execute("INSERT INTO " + tableName.getSQL() + " (id) VALUES (0)");
		conn.execute("INSERT INTO " + tableName.getSQL() + " VALUES (1)");
		conn.execute("INSERT INTO " + tableName.getSQL() + " (id) VALUES (1), (2)");
		conn.execute("INSERT INTO " + tableName.getSQL() + " VALUES (3), (4)");
		conn.execute("INSERT INTO " + tableName.getSQL() + " (id) VALUES (23), (24) ON DUPLICATE KEY UPDATE value = value + 1");

		conn.execute("INSERT INTO " + tableName.getQuoted() + " (id) VALUES (5)");
		conn.execute("INSERT INTO " + tableName.getQuoted() + " VALUES (6)");
		conn.execute("INSERT INTO " + tableName.getQuoted() + " (id) VALUES (7), (8)");
		conn.execute("INSERT INTO " + tableName.getQuoted() + " VALUES (9), (10)");

		conn.execute("USE " + dbName);
		conn.execute("INSERT INTO " + tableName.getUnqualified().getSQL() + " (id) VALUES (11)");
		conn.execute("INSERT INTO " + tableName.getUnqualified().getSQL() + " VALUES (12)");
		conn.execute("INSERT INTO " + tableName.getUnqualified().getSQL() + " (id) VALUES (13), (14)");
		conn.execute("INSERT INTO " + tableName.getUnqualified().getSQL() + " VALUES (15), (16)");

		conn.execute("INSERT INTO " + tableName.getUnqualified().getQuoted() + " (id) VALUES (17)");
		conn.execute("INSERT INTO " + tableName.getUnqualified().getQuoted() + " VALUES (18)");
		conn.execute("INSERT INTO " + tableName.getUnqualified().getQuoted() + " (id) VALUES (19), (20)");
		conn.execute("INSERT INTO " + tableName.getUnqualified().getQuoted() + " VALUES (21), (22)");
		conn.execute("INSERT INTO " + tableName.getSQL() + " (id) VALUES (25), (26) ON DUPLICATE KEY UPDATE value = value + 1");
	}
}
