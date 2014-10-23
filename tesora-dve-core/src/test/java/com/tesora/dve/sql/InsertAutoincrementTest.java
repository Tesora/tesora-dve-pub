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

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.variable.VariableConstants;
import com.tesora.dve.variables.KnownVariables;

public class InsertAutoincrementTest extends SchemaTest {

	private static final ProjectDDL checkDDL = new PEDDL("checkdb",
			new StorageGroupDDL("check", 1, "checkg"), "schema");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	protected ProxyConnectionResource conn;
	protected DBHelperConnectionResource dbh;

	@Before
	public void connect() throws Throwable {
		conn = new ProxyConnectionResource();
		checkDDL.create(conn);
		dbh = new DBHelperConnectionResource();
	}

	@After
	public void disconnect() throws Throwable {
		if(conn != null) {
			conn.disconnect();
			conn = null;
		}
		if(dbh != null) {
			dbh.disconnect();
			dbh = null;
		}
	}

	@Test
	public void test() throws Throwable {
		StringBuilder buf = new StringBuilder();
		buf.append("create table `autoinctab` (`id` int unsigned auto_increment, `junk` varchar(24), primary key (`id`)) ");
		conn.execute(buf.toString());

		// test standard insert still works
		conn.execute("insert into `autoinctab` (`junk`) values ('trash')");
		conn.assertResults("select * from `autoinctab`",
				br(nr, new Long(1), "trash"));

		// test multivalue insert works
		conn.execute("insert into `autoinctab` (`junk`) values ('trash1'),('trash2'),('trash3')");
		conn.assertResults(
				"select * from `autoinctab` where `junk` in ('trash1','trash2','trash3')",
				br(nr, new Long(2), "trash1", nr, new Long(3), "trash2", nr,
						new Long(4), "trash3"));

		// test specifying autoincrement works
		conn.execute("insert into `autoinctab` (`id`, `junk`) values (50, 'trash50')");
		conn.assertResults("select * from `autoinctab` where `id`=50",
				br(nr, new Long(50), "trash50"));

		// test specifying autoincrement works with multivalue insert
		conn.execute("insert into `autoinctab` (`id`, `junk`) values (51, 'trash51'),(52,'trash52'),(53,'trash53')");
		conn.assertResults(
				"select * from `autoinctab` where `junk` in ('trash51','trash52','trash53')",
				br(nr, new Long(51), "trash51", nr, new Long(52), "trash52",
						nr, new Long(53), "trash53"));

		// test new syntax
		conn.execute("set " + VariableConstants.REPL_SLAVE_INSERT_ID_NAME + "=99");
//		conn.execute("insert into `autoinctab` (`junk`) values ('new trash') auto_increment=99");
		conn.execute("insert into `autoinctab` (`junk`) values ('new trash')");
		conn.assertResults("select * from `autoinctab` where `id`=99",
				br(nr, new Long(99), "new trash"));

		// new syntax with multivalue insert
		conn.execute("set " + VariableConstants.REPL_SLAVE_INSERT_ID_NAME + "=100");
		conn.execute("insert into `autoinctab` (`junk`) values ('trash100'),('trash200'),('trash300')");
		conn.assertResults(
				"select * from `autoinctab` where `junk` in ('trash100','trash200','trash300')",
				br(nr, new Long(100), "trash100", nr, new Long(101),
						"trash200", nr, new Long(102), "trash300"));

		// make sure it throws an exception because the extended syntax cannot
		// be used if the auto_increment value is specified in the insert
		// statement
		try {
			conn.execute("insert into `autoinctab` (`id`, `junk`) values (500, 'trash500')");
			fail("Cannot specify autoincrement field and auto_increment= syntax");
		} catch (Exception e) {
			SchemaTest.assertSchemaException(e, "Cannot specify both the autoincrement column value and " + VariableConstants.REPL_SLAVE_INSERT_ID_NAME);
		}

		conn.execute("set " + VariableConstants.REPL_SLAVE_INSERT_ID_NAME + "=null");
		conn.execute("insert into `autoinctab` (`id`, `junk`) values (500, 'trash500')");
		conn.assertResults("select id from `autoinctab` where `junk` = 'trash500'",
				br(nr, new Long(500)));
	}

	@Test
	public void testMultiValueInsertExceedsMaxLiterals() throws Throwable {
		StringBuilder buf = new StringBuilder();
		buf.append("create table `autoinctab` (`id` int unsigned auto_increment, `junk` varchar(24), primary key (`id`)) ");
		conn.execute(buf.toString());

		Long orig = KnownVariables.CACHED_PLAN_LITERALS_MAX.getValue(null);
		try {
			KnownVariables.CACHED_PLAN_LITERALS_MAX.setValue(null, VariableScopeKind.GLOBAL, "1");

			conn.execute("insert into `autoinctab` (`junk`) values ('trash1'),('trash2'),('trash3')");
			conn.assertResults(
					"select * from `autoinctab` where `junk` in ('trash1','trash2','trash3')",
					br(nr, new Long(1), "trash1", nr, new Long(2), "trash2", nr, new Long(3), "trash3"));

			// PE-1253
			conn.execute("insert into `autoinctab` (`id`,`junk`) values (0,'trash4'),(0,'trash5'),(0,'trash6')");
			conn.assertResults(
					"select * from `autoinctab` where `junk` in ('trash4','trash5','trash6')",
					br(nr, new Long(4), "trash4", nr, new Long(5), "trash5", nr, new Long(6), "trash6"));

		} finally {
			KnownVariables.CACHED_PLAN_LITERALS_MAX.setValue(null, VariableScopeKind.GLOBAL, orig.toString());
        }
	}

	@Test
	public void testSpecifyIncrementValueIsRemovedFromCache() throws Throwable {
		final int NUM_INSERTS = 5;

		StringBuilder buf = new StringBuilder();
		buf.append("create table `autoinctab` (`id` int unsigned auto_increment, `junk` varchar(24), primary key (`id`)) ");
		conn.execute(buf.toString());

		// insert one record and determine the last insert id
		conn.execute("insert into `autoinctab` (`junk`) values ('trash')");

		ResourceResponse resp = conn.fetch("select @@last_insert_id");
		List<ResultRow> rows = resp.getResults();
		assertEquals("Expected one row only", 1, rows.size());

		Long lastInsertId = Long.valueOf((String) rows.get(0)
				.getResultColumn(1).getColumnValue());

		// insert and specify the autoincrement value of last insert id +
		// NUM_INSERTS
		Long specificInsertId = lastInsertId + NUM_INSERTS;
		resp = conn.execute("insert into `autoinctab` (`id`, `junk`) values ("
				+ specificInsertId + ", 'trash')");

		// if we do NUM_INSERTS more inserts without specifying the
		// autoincrement value we should
		// not reuse the specificInsertId and avoid a duplicate key insert
		// exception
		for (int i = 0; i < NUM_INSERTS; ++i) {
			resp = conn
					.execute("insert into `autoinctab` (`junk`) values ('trash')");
		}
	}

	@Test
	public void testNullLiterals() throws Throwable {
		conn.execute("create table `autoinctab` (`id` int unsigned auto_increment, `junk` varchar(24), primary key (`id`)) ");
		// get the cache warm
		conn.execute("insert into autoinctab (`junk`) values ('a')");
		conn.assertResults("select id from autoinctab where junk = 'a'", br(nr,new Long(1)));
		conn.execute("insert into autoinctab values (null, 'b')");
		conn.assertResults("select id from autoinctab where junk = 'b'", br(nr,new Long(2)));
		conn.execute("insert into autoinctab values (10, 'c')");
		conn.assertResults("select id from autoinctab where junk = 'c'", br(nr,new Long(10)));
		conn.execute("insert into autoinctab values(0, 'd')");
		conn.assertResults("select id from autoinctab where junk = 'd'", br(nr,new Long(11)));
		conn.execute("insert into autoinctab (`junk`) values ('e')");
		conn.assertResults("select id from autoinctab where junk = 'e'", br(nr, new Long(12)));
		conn.execute("insert into autoinctab values (20, 'f')");
		conn.assertResults("select id from autoinctab where junk = 'f'", br(nr, new Long(20)));
		conn.execute("insert into autoinctab values ('null', 'g')");
		conn.assertResults("select id from autoinctab where junk = 'g'", br(nr, new Long(21)));
		conn.execute("insert into autoinctab values (30, 'h'), (31, 'i')");
		conn.assertResults("select id from autoinctab where junk = 'i'", br(nr, new Long(31)));
		conn.execute("insert into autoinctab values (0, 'j'), (0, 'k')");
		conn.assertResults("select id from autoinctab where junk = 'k'", br(nr, new Long(33)));
		conn.execute("insert into autoinctab values ('0', 'l')");
		conn.assertResults("select id from autoinctab where junk = 'l'", br(nr, new Long(34)));
	}

	@Test
	public void testLastInsertId() throws Throwable {
		conn.execute("create table `a` (`id` int unsigned auto_increment, `junk` varchar(24), primary key (`id`)) ");
		conn.execute("insert into a (`junk`) values ('a')");
		conn.assertResults("select last_insert_id()", br(nr, Long.valueOf(1)));
		// when we get the right behaviour we can uncomment these lines out
//		conn.assertResults("select last_insert_id()", br(nr, Long.valueOf(1)));
//
//		conn.execute("create table `b` (`id` int unsigned auto_increment, `junk` varchar(24), primary key (`id`)) ");
//		conn.assertResults("select last_insert_id()", br(nr, Long.valueOf(1)));
//
//		conn.execute("insert into a (`junk`) values ('b')");
//		conn.assertResults("select last_insert_id()", br(nr, Long.valueOf(2)));
//
//		conn.execute("insert into b (`junk`) values ('c')");
//		conn.assertResults("select last_insert_id()", br(nr, Long.valueOf(1)));
//
//		conn.execute("insert into b (`id`, `junk`) values (99, '99')");
//		conn.assertResults("select last_insert_id()", br(nr, Long.valueOf(1)));
	}
}
