package com.tesora.dve.client;

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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.standalone.PETest;

@Test(groups = { "NonSmokeTest" })
public class ClientTestNG extends PETest {
	static DBHelper dbHelper;

	@BeforeClass
	public static void startup() throws Exception {

		TestCatalogHelper.createTestCatalog(PETest.class, 2);
		BootstrapHost.startServices(PETest.class);

        populateMetadata(ClientTestNG.class, Singletons.require(HostService.class).getProperties());
        populateSites(ClientTestNG.class, Singletons.require(HostService.class).getProperties());
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		BootstrapHost.stopServices();
	}

	@BeforeMethod
	public void preTestSetup() throws PEException {
		Properties peProps = PEFileUtils.loadPropertiesFile(ClientTestNG.class, PEConstants.CONFIG_FILE_NAME);

		dbHelper = new DBHelper(peProps.getProperty(PEConstants.PROP_JDBC_URL)
				+ "/TestDB?zeroDateTimeBehavior=convertToNull", peProps.getProperty(PEConstants.PROP_JDBC_USER),
				peProps.getProperty(PEConstants.PROP_JDBC_PASSWORD));
		dbHelper.connect();
	}

	@AfterMethod
	public void postTestTeardown() {
		dbHelper.disconnect();
	}

	@Test
	public void SimpleSelectTest1() throws Exception {

		dbHelper.executeQuery("SELECT * FROM alltypes");

		int rows = countResultSetRows(dbHelper.getResultSet());
		assertEquals(2, rows);
	}

	@Test
	public void showTypesTest1() throws Exception {
		int rows = countResultSetRows(dbHelper.getConnection().getMetaData().getTypeInfo());

		// TODO this is mysql specific......
		//
		assertEquals(40, rows);
	}

	@Test
	public void createTableTest() throws Exception {
		assertFalse(dbHelper.executeQuery("CREATE TABLE client_test1 ( col1 int, col2 varchar(10))"));

		assertTrue(dbHelper.executeQuery("SELECT * from client_test1"));
	}

	@Test
	public void simpleInsertTest() throws Exception {
		assertFalse(dbHelper.executeQuery("CREATE TABLE client_test2 ( col1 int, col2 varchar(10))"));
		assertFalse(dbHelper.executeQuery("INSERT INTO client_test2 VALUES (1,'row1 col2')"));

		assertTrue(dbHelper.executeQuery("SELECT * FROM client_test2"));

		int rows = countResultSetRows(dbHelper.getResultSet());
		assertEquals(1, rows);
	}

	@Test
	public void multipleTupleInsertTest() throws Exception {
		assertFalse(dbHelper.executeQuery("CREATE TABLE client_test ( col1 int, col2 varchar(10))"));
		assertFalse(dbHelper.executeQuery("INSERT INTO client_test VALUES (1,'row1 col2'),(2,'row2 col2')"));

		assertTrue(dbHelper.executeQuery("SELECT * FROM client_test"));

		int rows = countResultSetRows(dbHelper.getResultSet());
		assertEquals(2, rows);
	}

	@Test
	public void aliasingTest() throws Exception {
		assertFalse(dbHelper
				.executeQuery("CREATE TABLE atest1 (id int not null, rpart int not null, ipart int not null)"));
		assertFalse(dbHelper
				.executeQuery("insert into atest1 (id, rpart, ipart) values (1,1,0), (2,0,1),(3,1,1),(4,0,0)"));

		verifyMetadata("select * from atest1", new String[] { "id", "id", "rpart", "rpart", "ipart", "ipart" });
		verifyMetadata("select id di, rpart trapr, ipart trapi from atest1", new String[] { "id", "di", "rpart",
				"trapr", "ipart", "trapi" });
		verifyMetadata("select id di, rpart from atest1", new String[] { "id", "di", "rpart", "rpart" });
		verifyMetadata("select id, rpart + ipart from atest1", new String[] { "id", "id", "rpart + ipart",
				"rpart + ipart" });
		verifyMetadata("select id, rpart + ipart as s from atest1", new String[] { "id", "id", "s", "s" });
		verifyMetadata("select atest1.id, atest1.rpart, atest1.ipart as prati from atest1", new String[] { "id", "id",
				"rpart", "rpart", "ipart", "prati" });
		verifyMetadata("select a.id, case a.id when 0 then a.rpart else a.ipart end as cs from atest1 a", new String[] {
				"id", "id", "cs", "cs" });
	}

	@Test
	public void templateTest() throws Exception {
		dbHelper.executeQuery(new TemplateBuilder("drupal7")
			.withRequirement("create range if not exists block_range (int) persistent group #sg#")
			.withRequirement("create range if not exists field_range (varchar,int) persistent group #sg#")
			.toCreateStatement());
		assertFalse(dbHelper.executeQuery("CREATE DATABASE d7test USING TEMPLATE drupal7 STRICT"));
		assertTrue(dbHelper.executeQuery("SHOW RANGES"));
		assertEquals(2, countResultSetRows(dbHelper.getResultSet()));
	}

	@Test
	public void broadcastDeleteTest() throws Exception {
		assertFalse(dbHelper.executeQuery("CREATE TABLE foo_delete(col1 int) BROADCAST DISTRIBUTE"));
		assertFalse(dbHelper.executeQuery("INSERT INTO foo_delete VALUES (1)"));
		assertTrue(dbHelper.executeQuery("SELECT * FROM foo_delete"));
		assertEquals(1, countResultSetRows(dbHelper.getResultSet()));
		assertFalse(dbHelper.executeQuery("DELETE FROM foo_delete WHERE col1=1"));
		assertTrue(dbHelper.executeQuery("SELECT * FROM foo_delete"));
		assertEquals(0, countResultSetRows(dbHelper.getResultSet()));
	}

	@Test
	public void rollbackAfterStmtFailureTest() throws Exception {
		// This is to reproduce the
		// "commitTransaction called when no transaction in progress"
		// exception that occurs when a txn is inflight and a parser error
		// occurs.
		dbHelper.executeQuery("START TRANSACTION");

		try {
			dbHelper.executeQuery("SELECT 1 FROM WHERE cid=null");
		} catch (SQLException se) {
			// expected - ignore
		}

		dbHelper.executeQuery("ROLLBACK");

	}

	@Test
	public void defaultAndAutoIncrInsertTest() throws Exception {
		StringBuffer query = new StringBuffer().append("CREATE TABLE simpletest_test_id (")
				.append("`test_id` INT NOT NULL auto_increment,").append("`last_prefix` VARCHAR(60) NULL DEFAULT '',")
				.append("`null_column` VARCHAR(10) NULL,").append(" PRIMARY KEY (`test_id`))");
		dbHelper.executeQuery(query.toString());

		assertFalse(dbHelper.executeQuery("INSERT INTO simpletest_test_id (test_id) VALUES (default)"));
		assertFalse(dbHelper
				.executeQuery("INSERT INTO simpletest_test_id (last_prefix, null_column) VALUES (default, NULL), (default, 'nonull')"));
		assertFalse(dbHelper
				.executeQuery("INSERT INTO simpletest_test_id (test_id, last_prefix) VALUES (10,'hi'), (default,'there'), (default, default)"));
		assertEquals(3, dbHelper.getLastInsertID());

		assertTrue(dbHelper.executeQuery("SELECT * FROM simpletest_test_id"));
		int rows = countResultSetRows(dbHelper.getResultSet());
		assertEquals(6, rows);
	}

	@Test
	public void escapeProcessingTest() throws Exception {
		StringBuffer query = new StringBuffer().append("CREATE TABLE test_variable (")
				.append("`name` VARCHAR(128) NOT NULL,").append("`value` LONGBLOB NULL,")
				.append(" PRIMARY KEY (`name`)) BROADCAST DISTRIBUTE");
		dbHelper.executeQuery(query.toString());

		assertFalse(dbHelper.executeQuery("INSERT INTO test_variable (name) VALUES('drupal_test_email_collector')"));

		StringBuilder value = new StringBuilder().append('\\').append('\\').append("''").append("''").append("{`")
				.append('\\').append('\\').append('\\').append("'").append(";}}");
		String valueExp = StringEscapeUtils.unescapeJava(value.toString()).replaceAll("''", "'");

		StringBuilder update = new StringBuilder().append("UPDATE test_variable SET value='").append(value)
				.append("' WHERE ( (name = 'drupal_test_email_collector' ) )");
		assertFalse(dbHelper.executeQuery(update.toString()));

		assertTrue(dbHelper.executeQuery("SELECT value FROM test_variable WHERE name = 'drupal_test_email_collector'"));
		dbHelper.getResultSet().next();
		String valueFromDB = dbHelper.getResultSet().getString(1);

		assertTrue(StringUtils.equals(valueExp, valueFromDB));
	}

	@Test
	// Repro for PE-102
	public void largeInsertTest() throws Exception {
		final int NUM_TUPLES = 3300;
		final String ONE_TUPLE = "(0,' ', 'qqqqqqqqqqwwwwwwwwwweeeeeeeeeerrrrrrrrrrtttttttttt')";

		StringBuffer query = new StringBuffer().append("CREATE TABLE sbtest (")
				.append("id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,")
				.append("k INTEGER UNSIGNED DEFAULT '0' NOT NULL,").append("c char(120) DEFAULT '' NOT NULL,")
				.append("pad char(60) DEFAULT '' NOT NULL,").append(" PRIMARY KEY (id))");
		dbHelper.executeQuery(query.toString());

		StringBuffer insert = new StringBuffer().append("INSERT INTO sbtest (k, c, pad) VALUES ");

		boolean doneFirst = false;
		for (int i = 0; i < NUM_TUPLES; i++) {
			if (doneFirst)
				insert.append(",");
			else
				doneFirst = true;

			insert.append(ONE_TUPLE);
		}
		insert.append(";");

		dbHelper.executeQuery(insert.toString());

		dbHelper.executeQuery("SELECT COUNT(*) FROM sbtest");
		dbHelper.getResultSet().next();
		assertEquals(NUM_TUPLES, dbHelper.getResultSet().getLong(1));
	}

	// repro for PE-268
	@Test
	public void dateTypeTest() throws Exception {
		final String dateValue = "2012-10-03";
		final String timeValue = "15:49:36";
		final String dateTimeValue = dateValue + " " + timeValue + ".0"; // need a .0 for mS on the end of datetime
	
		StringBuffer query = new StringBuffer().append("CREATE TABLE test_date (").append("`datecol` date NULL,")
				.append("`timecol` time NULL, ").append("`datetimecol` datetime NULL)");
		dbHelper.executeQuery(query.toString());

		query = new StringBuffer().append("INSERT INTO test_date VALUES ('").append(dateValue).append("','")
				.append(timeValue).append("','").append(dateTimeValue).append("')");
		dbHelper.executeQuery(query.toString());

		dbHelper.executeQuery("SELECT * FROM test_date");
		ResultSet rs = dbHelper.getResultSet();
		rs.next();
		assertEquals(dateValue, rs.getString(1));
		assertEquals(timeValue, rs.getString(2));
		assertEquals(dateTimeValue, rs.getString(3));
	}

	// repro for PE-337
	@Test
	public void zeroDateTest() throws Exception {
		final String zeroDateTime = "0000-00-00 00:00:00";
		final String zeroDate = "0000-00-00";
		final String zeroTime = "00:00:00";

		StringBuffer query = new StringBuffer().append("CREATE TABLE wp_posts (")
				.append("ID bigint(20) unsigned NOT NULL,").append("post_datetime datetime NOT NULL default '")
				.append(zeroDateTime).append("',").append("post_date date NOT NULL default '").append(zeroDate)
				.append("',").append("post_time time NOT NULL default '").append(zeroTime).append("',")
				.append("post_timestamp timestamp NOT NULL default '").append(zeroDateTime).append("')");
		dbHelper.executeQuery(query.toString());

		dbHelper.executeQuery("insert into wp_posts (ID) values (1)");

		dbHelper.executeQuery("select post_datetime, post_date, post_time, post_timestamp from wp_posts");
		ResultSet rs = dbHelper.getResultSet();
		rs.next();
		assertNull(rs.getString(1));
		assertNull(rs.getString(2));
		assertEquals(zeroTime, rs.getString(3));
		assertNull(rs.getString(4));
	}

	private static void verifyMetadata(String query, String[] namesAndAliases) throws Exception {
		assertTrue(dbHelper.executeQuery(query));
		ResultSetMetaData rsmd = dbHelper.getResultSet().getMetaData();
		verifyMetadata(rsmd, namesAndAliases);
	}

	private static void verifyMetadata(ResultSetMetaData rsmd, String[] namesAndAliases) throws SQLException {
		if (namesAndAliases == null) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				System.out.println(rsmd.getColumnName(i) + " as " + rsmd.getColumnLabel(i));
			}
			return;
		}
		int ncolumns = namesAndAliases.length / 2;
		assertEquals(ncolumns, rsmd.getColumnCount());
		for (int i = 0; i < ncolumns; i++) {
			String ename = namesAndAliases[2 * i];
			String ealias = namesAndAliases[2 * i + 1];
			assertEquals(ename, rsmd.getColumnName(i + 1));
			assertEquals(ealias, rsmd.getColumnLabel(i + 1));
		}
	}

	@Test
	public void infoSchemaColumnMetadataTest() throws Exception {
		StringBuffer query = new StringBuffer().append("SHOW DATABASES");
		dbHelper.executeQuery(query.toString());

		ResultSet rs = dbHelper.getResultSet();
		ResultSetMetaData rsmd = rs.getMetaData();

		// make sure the column header is not empty
		assertTrue(!StringUtils.isEmpty(rsmd.getColumnLabel(1)));
		assertEquals(ShowSchema.Database.NAME, rsmd.getColumnLabel(1));
	}

	private void loadSQLFile(Class<?> testClass, String fileName) throws Exception {
		InputStream is = testClass.getResourceAsStream(fileName);
		if (is != null) {
			logger.info("Reading SQL statements from " + fileName);
			dbHelper.setDisconnectAfterProcess(false);
			dbHelper.executeFromStream(is);
			try {
				is.close();
			} catch (IOException e) {
			}
		}
	}

	// repro for PE-481
	@Test(enabled = false)
	public void generationAddTest() throws Exception {
		loadSQLFile(ClientTestNG.class, "large-table-load.sql");

		dbHelper.executeQuery("ALTER PERSISTENT GROUP " + PEConstants.DEFAULT_GROUP_NAME
				+ " ADD GENERATION site3,site4,site5,site6");

		dbHelper.executeQuery("SHOW GENERATION SITES WHERE persistent_group='" + PEConstants.DEFAULT_GROUP_NAME + "'");
		assertEquals(6, countResultSetRows(dbHelper.getResultSet()));
	}
	
	@Test
	public void pe1428Test() throws Exception {
		dbHelper.executeQuery("CREATE RANGE test1range (int) persistent group " + PEConstants.DEFAULT_GROUP_NAME);
		dbHelper.executeQuery("CREATE TABLE test1(col1 int) RANGE DISTRIBUTE ON (col1) USING test1range");
		dbHelper.executeQuery("INSERT INTO test1 VALUES (1),(2),(3),(4),(5)");
		
		dbHelper.executeQuery("SELECT count(*), @dve_sitename FROM test1 GROUP BY 2");
		assertEquals(2, countResultSetRows(dbHelper.getResultSet()));
		
		dbHelper.executeQuery("DROP TABLE test1");
		dbHelper.executeQuery("DROP RANGE test1range");
		
	}
}
