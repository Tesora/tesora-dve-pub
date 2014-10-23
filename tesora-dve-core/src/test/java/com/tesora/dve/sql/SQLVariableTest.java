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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.parser.TimestampVariableUtils;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.util.ComparisonOptions;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ProxyConnectionResourceResponse;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.VariableManager;

public class SQLVariableTest extends SchemaTest {

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",2,"sysg"),
				"schema");
	
	private static final String nonRootUser = "ben";
	private static final String nonRootAccess = "localhost";
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(sysDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		sysDDL.create(pcr);
		removeUser(pcr,nonRootUser,nonRootAccess);
		pcr.execute(String.format("create user '%s'@'%s' identified by '%s'",nonRootUser,nonRootAccess,nonRootUser));
		pcr.disconnect();
	}

	ProxyConnectionResource conn = null;
	
	@Before
	public void before() throws Throwable {
		conn = new ProxyConnectionResource();
	}
	
	@After
	public void after() throws Throwable {
		conn.disconnect();
	}
	
	@Test
	public void testA() throws Throwable {
		conn.execute("set names latin1");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", "latin1",
				   nr,"character_set_connection", "latin1",
				   nr,"character_set_database","utf8",
				   nr,"character_set_results","latin1",
				   nr,"character_set_server","utf8",
				   nr,"character_set_system","utf8"));
		conn.assertResults("show variables like 'character%'",
				br(nr,"character_set_client", "latin1",
				   nr,"character_set_connection", "latin1",
				   nr,"character_set_database","utf8",
				   nr,"character_set_results","latin1",
				   nr,"character_set_server","utf8",
				   nr,"character_set_system","utf8"));
		conn.execute("set @prevcharset = @@character_set_connection");
		conn.execute("set names utf8");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", "utf8",
				   nr,"character_set_connection", "utf8",
				   nr,"character_set_database","utf8",
				   nr,"character_set_results","utf8",
				   nr,"character_set_server","utf8",
				   nr,"character_set_system","utf8"));
		conn.assertResults("select @@character_set_client,@@session.character_set_connection,@@character_set_results",br(nr,"utf8","utf8","utf8"));
		conn.execute("set @@character_set_connection = @prevcharset");
		conn.assertResults("select @@character_set_connection",br(nr,"latin1"));
        conn.assertResults("show dve variables like 'version'",br(nr,"version", Singletons.require(HostService.class).getDveServerVersion()));
		try {
			// this should fail
			conn.execute("set version = '1.0'");
			fail("SET VERSION command should have thrown an exception");
		} catch (PEException re) {
			assertException(re,PEException.class,"Variable 'version' not settable as session variable");
		}		
	}
	
	@Test
	public void testB() throws Throwable {
		conn.execute("use " + sysDDL.getDatabaseName());
		conn.execute("create table `vtest` (`id` int, `pl` varchar(32))");
		conn.execute("set @default_payload = 'this is some text'");
		conn.execute("set @default_id = 15");
		conn.assertResults("show variables like 'tx_isolation'",br(nr,"tx_isolation","READ-COMMITTED"));
		conn.assertResults("select @@tx_isolation",br(nr,"READ-COMMITTED"));
		conn.execute("set transaction isolation level serializable");
		conn.assertResults("show variables like 'tx_isolation'", br(nr,"tx_isolation","SERIALIZABLE"));
		conn.assertResults("select @@tx_isolation",br(nr,"SERIALIZABLE"));
		conn.execute("insert into `vtest` (`id`, `pl`) values (1,'one'),(2,@default_payload),(@default_id, 'three')");
		conn.assertResults("select * from vtest where id = 15",br(nr,new Integer(15),"three"));
		conn.assertResults("select id from vtest where pl = @default_payload",br(nr,new Integer(2)));
	}

	@Test
	public void testShowVariables() throws Throwable {
		ResourceResponse rr = conn.fetch("show variables");
		List<ResultRow> results = rr.getResults();
		for(ResultRow row : results) {
			assertFalse(row.getResultColumn(1).getColumnValue() + " should have a value",row.getResultColumn(2).isNull());
		}
		
		// PE-668 needs this for reports to work
		rr = conn.fetch("show /*!50000 global */ variables");
		results = rr.getResults();
		for(ResultRow row : results) {
			//skip the aws ones
			if (!StringUtils.containsIgnoreCase((String)row.getResultColumn(1).getColumnValue(), "aws")) {
				assertFalse(row.getResultColumn(1).getColumnValue() + " should have a value",row.getResultColumn(2).isNull());
			}
		}
	}
	
	// when we actually support the where clause - that's the point at which we enable this again
	@Ignore
	@Test
	public void testShowVariablesFilter() throws Throwable {
		ResourceResponse unfiltered = conn.fetch("show variables");
		ResourceResponse filtered = conn.fetch("show variables where Variable_name = 'version_comment'");
		unfiltered.assertEqualResults("testShowVariablesFilter", filtered, ComparisonOptions.DEFAULT.withIgnoreOrder());
	}
	
	@Test
	public void setNamesWithVariousQuotes() throws Throwable {

		// PE-144 support double quotes for set names
		// so support no quotes, single and double quotes
		String charSet = "latin1";
		conn.execute("set names " + charSet);
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", charSet,
				   nr,"character_set_connection", charSet,
				   nr,"character_set_database","utf8",
				   nr,"character_set_results",charSet,
				   nr,"character_set_server","utf8",
				   nr,"character_set_system","utf8"
				   ));
		
		charSet = "utf8";
		conn.execute("set names " + "'" + charSet + "'");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", charSet,
				   nr,"character_set_connection", charSet,
				   nr,"character_set_database","utf8",
				   nr,"character_set_results",charSet,
				   nr,"character_set_server","utf8",
				   nr,"character_set_system","utf8"
				   ));
		
		charSet = "ascii";
		conn.execute("set names " + "\"" + charSet + "\"");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", charSet,
				   nr,"character_set_connection", charSet,
				   nr,"character_set_database","utf8",
				   nr,"character_set_results",charSet,
				   nr,"character_set_server","utf8",
				   nr,"character_set_system","utf8"
				   ));
	}

	@Test
	public void testPE293() throws Throwable {

		// PE-293 support CHARSET, CHARACTER SET and CHAR SET
		String charSet = "utf8";
		String command = "CHARSET";
		conn.execute("set " + command + " " + charSet);
		conn.assertResults("show variables like 'charset'",
				br(nr,"charset", charSet));
		
		charSet = "latin1";
		command = "CHAR SET";
		conn.execute("set " + command + " " + charSet);
		conn.assertResults("show variables like 'charset'",
				br(nr,"charset", charSet));
		
		charSet = "ascii";
		command = "CHARACTER SET";
		conn.execute("set " + command + " " + charSet);
		conn.assertResults("show variables like 'charset'",
				br(nr,"charset", charSet));
	}

	@Test
	public void testShowPlugins() throws Throwable {

		ProxyConnectionResourceResponse rr = (ProxyConnectionResourceResponse) conn.fetch("show plugins");
		
		// can't check the data returned cause it may vary on different machines
		// will check the header and number of columns
		String[] colHeaders = new String[] {"Name", "Status", "Type", "Library", "License"};
		List<String> returnedColHeaders = new ArrayList<String>();
		ColumnSet cs = rr.getColumns();
		for (int i = 1; i <= cs.size(); i++) {
			returnedColHeaders.add(cs.getColumn(i).getAliasName().trim());
		}
		for (int i = 0; i < colHeaders.length; i++) {
			returnedColHeaders.contains(colHeaders[i]);
			assertTrue("Column " + colHeaders[i] + " must be one of the column headers for show plugins", 
					returnedColHeaders.contains(colHeaders[i]));
		}

		assertTrue("Must have at least one row from a show plugin", rr.getResults().size() > 0);
	}
	
	@Test
	public void testExtensionComments() throws Throwable {
		
		conn.execute("/*!40103 set @OLD_TIME_ZONE=@@TIME_ZONE */");
		conn.execute("/*!40103 set TIME_ZONE='+00:30' */");
		conn.assertResults("select @@TIME_ZONE", br(nr,"+00:30"));
		conn.execute("/*!40103 set TIME_ZONE=@OLD_TIME_ZONE */");
		conn.assertResults("select /* comment */ @@TIME_ZONE /* another comment */", br(nr,"+00:00"));
		
	}
	
	@Test
	public void testDisabledVariables() throws Throwable {

		// should not be able to set sql_auto_is_null to 1
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set sql_auto_is_null = 1");
			}
		}.assertError(SchemaException.class, MySQLErrors.internalFormatter,
					"Internal error: No support for sql_auto_is_null = 1 (planned)");
		conn.execute("set sql_auto_is_null = 0");
	}
	
	@Test
	public void testPE378() throws Throwable {
		conn.execute("set sql_safe_updates = 1");
		conn.execute("set sql_safe_updates = 0");
		conn.execute("set sql_safe_updates = 'can be anything cause we ignore this variable'");
	}

	@Test
	public void testSetTxIsolationLevel() throws Throwable {
		final String variableName = "tx_isolation";

		assertVariableValue(variableName, "READ-COMMITTED");
		conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
		assertVariableValue(variableName, "SERIALIZABLE");
		conn.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
		assertVariableValue(variableName, "READ-COMMITTED");
	}

	@Test
	public void testSqlModeDefault() throws Throwable {
		final String variableName = "sql_mode";

		// Reset the GLOBAL value first to test for PE-1608.
		assertVariableValue(variableName, "NO_ENGINE_SUBSTITUTION");
		conn.execute("SET GLOBAL sql_mode=default;");
		assertVariableValue(variableName, "NO_ENGINE_SUBSTITUTION");

		conn.execute("SET sql_mode=ALLOW_INVALID_DATES;");
		assertVariableValue(variableName, "NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES");
		conn.execute("SET sql_mode=default;");
		assertVariableValue(variableName, "NO_ENGINE_SUBSTITUTION");
		conn.execute("SET sql_mode=\" NO_AUTO_CREATE_USER , no_engine_substitution , ALLOW_INVALID_DATES \";");
		assertVariableValue(variableName, "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES");
		conn.execute("SET sql_mode=\"\";");
		assertVariableValue(variableName, "NO_ENGINE_SUBSTITUTION");
	}

	@Test
	public void testPE1586() throws Throwable {
		assertVariableValue("lower_case_file_system", "OFF");
	}

	@Test
	public void testPE1587() throws Throwable {
		assertVariableValue("have_query_cache", "YES");
	}

	@Test
	public void testPE1589() throws Throwable {
		assertVariableValue("have_ssl", "NO");
		assertVariableValue("have_openssl", "NO");
	}

	@Test
	public void testPE1590() throws Throwable {
		assertVariableValue("thread_handling", "one-thread-per-connection");
	}

	@Test
	public void testPE1609() throws Throwable {
		assertVariableValue("time_format", "%H:%i:%s");
	}

	@Test
	public void testPE1610() throws Throwable {
		assertVariableValue("thread_concurrency", "10");
	}

	@Test
	public void testPE1611() throws Throwable {
		assertVariableValue("log_bin", "OFF");
	}

	@Test
	public void testPE1612() throws Throwable {
		assertVariableValue("license", "AGPL");
	}

	@Test
	public void testPE1613() throws Throwable {
		final String serverVersion = getVariableValue("version");
		assertVariableValue("innodb_version", serverVersion);
	}

	@Test
	public void testPE1614() throws Throwable {
		assertVariableValue("ignore_builtin_innodb", "OFF");
	}

	@Test
	public void testPE1615() throws Throwable {
		assertVariableValue("have_profiling", "NO");
	}

	@Test
	public void testPE1616() throws Throwable {
		assertVariableValue("datetime_format", "%Y-%m-%d %H:%i:%s");
	}

	@Test
	public void testPE1617() throws Throwable {
		assertVariableValue("date_format", "%Y-%m-%d");
	}

	@Test
	public void testPE1618() throws Throwable {
		assertVariableValue("character_set_system", "utf8");
	}

	@Test
	public void testPE1619() throws Throwable {
		assertVariableValue("div_precision_increment", "4");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set session div_precision_increment = 31");
			}
		}.assertException(PEException.class, "Invalid value '31' must be no more than 30 for variable 'div_precision_increment'");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set session div_precision_increment = -1");
			}
		}.assertException(PEException.class, "Invalid value '-1' must be at least 0 for variable 'div_precision_increment'");

		conn.assertResults("SELECT 1/7", br(nr, BigDecimal.valueOf(0.1429)));

		conn.execute("set session div_precision_increment = 12");

		conn.assertResults("SELECT 1/7", br(nr, BigDecimal.valueOf(0.142857142857)));
	}

	@Test
	public void testPE1620() throws Throwable {
		assertVariableValue("default_week_format", "0");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set session default_week_format = 8");
			}
		}.assertException(PEException.class, "Invalid value '8' must be no more than 7 for variable 'default_week_format'");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set session default_week_format = -1");
			}
		}.assertException(PEException.class, "Invalid value '-1' must be at least 0 for variable 'default_week_format'");

		conn.assertResults("SELECT WEEK('2008-02-20')", br(nr, 7L));

		conn.execute("set session default_week_format = 1");

		conn.assertResults("SELECT WEEK('2008-02-20')", br(nr, 8L));
		conn.assertResults("SELECT WEEK('2008-12-31')", br(nr, 53L));
	}

	@Test
	public void testPE1621() throws Throwable {
		assertVariableValue("version_compile_machine", "64-bit");
	}

	@Test
	public void testPE1603() throws Throwable {
		assertTimestampValue(2, null);

		conn.execute("set session timestamp = 10");

		assertTimestampValue(2, 10l);

		conn.execute("set session timestamp = 0");

		assertTimestampValue(2, null);

		conn.execute("set session timestamp = 300000000");

		assertTimestampValue(2, 300000000l);

		conn.execute("set session timestamp = DEFAULT");

		assertTimestampValue(2, null);

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set session timestamp = '10'");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongTypeForVariable, "timestamp");

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set session timestamp = 'DEFAULT'");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongTypeForVariable, "timestamp");
	}

	private void assertTimestampValue(final int waitTimeSec, final Long expected) throws Throwable {
		final Long value1 = Long.parseLong(getVariableValue("timestamp"));
		Thread.sleep(Long.valueOf(1000 * waitTimeSec));
		final Long value2 = Long.parseLong(getVariableValue("timestamp"));

		if (expected != null) {
            //this timestamp checks are simple, because set timestamp whould fix the returned value of NOW()
			conn.assertResults("SELECT UNIX_TIMESTAMP(NOW())", br(nr, expected));
			assertEquals(expected, value1);
			assertEquals(expected, value2);
		} else {
            //these timestamp checks have to account for clock and execution drift, since the timestamp is set to DEFAULT.

            assertTimestampBetween(value1, value1 + waitTimeSec, value2, /*allowed width*/waitTimeSec+2);//allow for 2 seconds of variation, Thread.sleep(2000) could easily measure as 3 seconds.

            long beforeTime = TimestampVariableUtils.getCurrentSystemTime();
            ResourceResponse queriedTimestamp = conn.fetch("SELECT UNIX_TIMESTAMP(NOW())");
            long afterTime = TimestampVariableUtils.getCurrentSystemTime();
            List<ResultRow> rows = queriedTimestamp.getResults();
            assertEquals(1,rows.size());
            final ResultRow row = rows.get(0);
            final List<ResultColumn> resultColumns = row.getRow();
            assertEquals(1,resultColumns.size());
            ResultColumn col = resultColumns.get(0);
            long entry = (Long)col.getColumnValue();

			assertTimestampBetween(beforeTime,entry,afterTime, 1);

		}
	}

    private void assertTimestampBetween(long before, long middle, long after, long allowedWidth){
        assertTrue("before = "+before +" , middle= "+middle , before <= middle);
        assertTrue("middle = " + middle + " , after = " + after, middle <= after);
        assertTrue("before = "+before +" , after = " + after +" , allowedWidth = "+allowedWidth, after - before <= allowedWidth);
    }

	@Test
	public void testPE1639() throws Throwable {
		assertVariableValue("innodb_adaptive_flushing", "ON");
	}

	@Test
	public void testPE1640() throws Throwable {
		assertVariableValue("innodb_fast_shutdown", "1");
	}

	@Test
	public void testPE1641() throws Throwable {
		assertVariableValue("innodb_mirrored_log_groups", "1");
	}

	@Test
	public void testPE1642() throws Throwable {
		assertVariableValue("innodb_stats_method", "nulls_equal");
	}

	@Test
	public void testPE1643() throws Throwable {
		final String tspValue = getVariableValue("innodb_stats_transient_sample_pages");
		assertVariableValue("innodb_stats_sample_pages", tspValue);
	}

	@Test
	public void testPE1644() throws Throwable {
		assertVariableValue("innodb_stats_transient_sample_pages", "8");
	}

	@Test
	public void testPE1645() throws Throwable {
		assertVariableValue("innodb_table_locks", "ON");
	}

	@Test
	public void testPE1646() throws Throwable {
		assertVariableValue("myisam_use_mmap", "OFF");
	}

	@Test
	public void testPE1647() throws Throwable {
		assertVariableValue("tmp_table_size", "16777216");
	}

	@Test
	public void testPE1649() throws Throwable {
		assertVariableValue("skip_networking", "OFF");
	}

	@Test
	public void testPE1650() throws Throwable {
		assertVariableValue("protocol_version", "10");
	}

	@Test
	public void testPE1656() throws Throwable {
		assertVariableValue("backend_wait_timeout", "28800");
		assertVariableValue("wait_timeout", "28800");

		conn.execute("set wait_timeout = 14400");
		assertVariableValue("backend_wait_timeout", "28800");
		assertVariableValue("wait_timeout", "14400");
	}

	@Test
	public void testPE1659() throws Throwable {
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set global wait_timeout = 'blah'");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongTypeForVariable, "wait_timeout");

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set global sql_auto_is_null = 2");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongValueForVariable, "sql_auto_is_null", "2");

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set global sql_auto_is_null = 'blah'");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongValueForVariable, "sql_auto_is_null", "blah");

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set global tx_isolation = 'blah'");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongValueForVariable, "tx_isolation", "blah");

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("set global tx_isolation = NULL");
			}
		}.assertError(SchemaException.class, MySQLErrors.wrongValueForVariable, "tx_isolation", "NULL");
	}

	@Test
	public void testPE1662() throws Throwable {

		// SESSION values
		assertVariableValue("character_set_connection", "utf8");
		assertVariableValue("collation_connection", "utf8_general_ci");

		conn.execute("set character_set_connection = ascii");

		assertVariableValue("character_set_connection", "ascii");
		assertVariableValue("collation_connection", "ascii_general_ci");

		conn.execute("set collation_connection = utf8_general_ci");

		assertVariableValue("character_set_connection", "utf8");
		assertVariableValue("collation_connection", "utf8_general_ci");

		// GLOBAL values
		assertGlobalVariableValue("character_set_connection", "utf8");
		assertGlobalVariableValue("collation_connection", "utf8_general_ci");

		conn.execute("set global character_set_connection = ascii");

		assertGlobalVariableValue("character_set_connection", "ascii");
		assertGlobalVariableValue("collation_connection", "ascii_general_ci");

		conn.execute("set global collation_connection = utf8_general_ci");

		assertGlobalVariableValue("character_set_connection", "utf8");
		assertGlobalVariableValue("collation_connection", "utf8_general_ci");
	}

	private void assertVariableValue(final String variableName, final Object expected) throws Throwable {
		conn.assertResults("show variables like '" + variableName + "'", br(nr, variableName, expected));
	}
	
	private void assertGlobalVariableValue(final String variableName, final Object expected) throws Throwable {
		conn.assertResults("show global variables like '" + variableName + "'", br(nr, variableName, expected));
	}

	private String getVariableValue(final String variableName) throws Throwable {
		final List<ResultRow> rows = conn.fetch("show variables like '" + variableName + "'").getResults();
		assertEquals("Exactly one result row expected for variable '" + variableName + "'.", 1, rows.size());

		final ResultRow row = rows.get(0);
		final List<ResultColumn> resultColumns = row.getRow();
		if (resultColumns.size() != 2) {
			throw new PECodingException("\"SHOW VARIABLES LIKE ...\" should return exactly two columns.");
		}
		assertEquals("Wrong variable name returned for '" + variableName + "'.", variableName, resultColumns.get(0).getColumnValue());

		return (String) resultColumns.get(1).getColumnValue();
	}

	// this is a cheesy test
	@Test
	public void testDynamicAdd() throws Throwable {
		String infoQuery = "select * from information_schema.variable_definitions where name like 'fromage'";
		conn.assertResults(infoQuery,br());
		conn.execute("alter dve add variable fromage scopes='SESSION,GLOBAL' valuetype='varchar' value='blue' options='NULLABLE' description='cheese!'");
		conn.assertResults(infoQuery, br(nr,"fromage","blue","varchar","SESSION,GLOBAL","NULLABLE","cheese!"));
		ProxyConnectionResource nonRoot = new ProxyConnectionResource(nonRootUser,nonRootUser);
		try {
			nonRoot.assertResults("show variables like 'fromage'", br(nr,"fromage","blue"));
		} finally {
			nonRoot.close();
		}
	}
	
	@Test
	public void testDocStrings() throws Throwable {
		ResourceResponse rr = conn.execute("select name from information_schema.variable_definitions where description = ''");
		List<String> missing = new ArrayList<String>();
		for(ResultRow row : rr.getResults()) {
			missing.add((String)row.getResultColumn(1).getColumnValue());
		}
		if (!missing.isEmpty()) 
			fail("Empty variable docstrings: " + Functional.join(missing, ","));
//		System.out.println(conn.printResults("select * from information_schema.variable_definitions"));
	}
	
	private static String getCurrentGlobalValue(DBHelper helper, String varName) throws Throwable {
		ResultSet rs = null;
		String out = null;
		try {
			if (helper.executeQuery("select @@global." + varName)) {
				rs = helper.getResultSet();
				if (!rs.next())
					fail("Variable " + varName + " apparently does not exist on native");
				out = rs.getString(1);
			}
		} finally {
			if (rs != null) 
				rs.close();
		}
		return out;
	}
	
	@Test
	public void testGlobalPushdown() throws Throwable {
		VariableManager vm = Singletons.require(HostService.class).getVariableManager();
		DBHelper helper = null;
		
		String execFormat = "set global %s = %s";
		
		try {
			helper = buildHelper();
			for(VariableHandler vh : vm.getGlobalHandlers()) {
				if (!vh.isEmulatedPassthrough()) continue;
				Object defVal = vh.getDefaultOnMissing();
				if (defVal == null) continue;
				String currentGlobal = getCurrentGlobalValue(helper,vh.getName());
				String setTo = vh.toExternal(defVal);
				conn.execute(String.format(execFormat,vh.getName(),setTo));
				String newGlobal = getCurrentGlobalValue(helper,vh.getName());
				assertEquals("should have same value for " + vh.getName(),PEStringUtils.dequote(setTo),newGlobal);
				Object oldGlobalConverted = vh.toInternal(currentGlobal);
				String oldGlobalExternal = vh.toExternal(oldGlobalConverted);
				helper.executeQuery("set global " + vh.getName() + " = " + oldGlobalExternal);
			}
		} finally {
			helper.disconnect();
		}
		
	}
	
	@Test
	public void testAccess() throws Throwable {
		VariableManager vm = Singletons.require(HostService.class).getVariableManager();
		testAccess(vm.lookupMustExist(null,"tx_isolation"),new Values("REPEATABLE-READ","SERIALIZABLE","READ-COMMITTED"));
		testAccess(vm.lookupMustExist(null,"adaptive_cleanup_interval"), new Values("1000","5000","10000"));
		testAccess(vm.lookupMustExist(null,"cost_based_planning"),new Values("YES","NO"));
		testAccess(vm.lookupMustExist(null,"debug_context"), new Values("YES","NO"));
	}
	
	private static final VariableRoundTrip[] globals = new VariableRoundTrip[] {
		new VariableRoundTrip(
				"set global %s = %s",
				"show global variables like '%s'") {

			public Object[] buildExpectedResults(String varName, String varValue) {
				return br(nr,varName,varValue);
			}
		},
		new VariableRoundTrip(
				"set @@global.%s = %s",
				"select variable_value from information_schema.global_variables where variable_name like '%s'")
	};

	private static final VariableRoundTrip[] sessions = new VariableRoundTrip[] {
		new VariableRoundTrip(
				"set %s = %s",
				"show session variables like '%s'") {
			public Object[] buildExpectedResults(String varName, String varValue) {
				return br(nr,varName,varValue);
			}
			
		},
		new VariableRoundTrip(
				"set @@session.%s = %s",
				"select variable_value from information_schema.session_variables where variable_name like '%s'"),
		new VariableRoundTrip(
				"set @@%s = %s",
				"select variable_value from information_schema.session_variables where variable_name like '%s'"),
		new VariableRoundTrip(
				"set @@local.%s = %s",
				"select variable_name, variable_value from information_schema.session_variables where variable_name = '%s'") {
			public Object[] buildExpectedResults(String varName, String varValue) {
				return br(nr,varName,varValue);
			}			
		}
	};
	
	private static final String sessQuery = 
			"select variable_value from information_schema.session_variables where variable_name = '%s'";
			
	private void testAccess(VariableHandler handler, Values values) throws Throwable {
		ProxyConnectionResource nonRoot = new ProxyConnectionResource(nonRootUser,nonRootUser);
		try {
			// before we start modifying the global values, save off the session value to validate
			// that it does not change
			String sessVal = null;
			if (handler.getScopes().contains(VariableScopeKind.SESSION)) 
				sessVal = getSessionValue(nonRoot,handler);
			if (handler.getScopes().contains(VariableScopeKind.GLOBAL)) {
				// make sure all the root versions work
				for(VariableRoundTrip vrt : globals) {
					roundTrip(conn,handler,vrt,values);
				}
				// now, for each root version, make sure a nonroot user cannot set it
				for(VariableRoundTrip vrt : globals) {
					try {
						nonRoot.execute(vrt.buildSet(handler.getName(), values.current(), handler.getMetadata().isNumeric()));
						fail("non root should not be able to set global value of " + handler.getName());
					} catch (PEException pe) {
						if (!pe.getMessage().startsWith("Must be root"))
							throw pe;
					}
				}
			}
			if (handler.getScopes().contains(VariableScopeKind.SESSION)) {
				String stillValue = getSessionValue(nonRoot,handler);
				assertEquals("sess value should not change",sessVal,stillValue);
				
				// make sure both root and nonroot can modify session values
				for(VariableRoundTrip vrt : sessions) {
					roundTrip(conn,handler,vrt,values);
				}
				for(VariableRoundTrip vrt : sessions) {
					roundTrip(nonRoot,handler,vrt,values);
				}
			}
		} finally {
			nonRoot.disconnect();
		}
	}

	private String getSessionValue(ProxyConnectionResource conn, VariableHandler handler) throws Throwable {
		ResourceResponse rr = conn.execute(String.format(sessQuery,handler.getName()));
		return (String) rr.getResults().get(0).getRow().get(0).getColumnValue();		
	}
	
	private void roundTrip(ProxyConnectionResource proxyConn, VariableHandler handler,
			VariableRoundTrip vrt, Values values) throws Throwable {
		conn.execute(vrt.buildSet(handler.getName(), values.next(), handler.getMetadata().isNumeric()));
		conn.assertResults(vrt.buildQuery(handler.getName()),
				vrt.buildExpectedResults(handler.getName(), values.current()));
	}
	
	private static class Values {
		
		private final String[] values;
		private int index = -1;
		
		public Values(String... myValues) {
			values = myValues;
			index = -1;
		}
		
		public String next() {
			if (++index >= values.length)
				index = 0;
			return values[index];
		}
		
		public String current() {
			return values[index];
		}
	}
	
	private static class VariableRoundTrip {
		
		private String setFormat;
		private String queryFormat;
		
		public VariableRoundTrip(String setFormat, String queryFormat) {
			this.setFormat = setFormat;
			this.queryFormat = queryFormat;
		}
		
		public String buildSet(String varName, String varValue, boolean isNumeric) {
			return String.format(setFormat, varName, (isNumeric) ? varValue : PEStringUtils.singleQuote(varValue));
		}
		
		public String buildQuery(String varName) {
			return String.format(queryFormat, varName);
		}
		
		public Object[] buildExpectedResults(String varName, String varValue) {
			return br(nr,varValue);
		}
		
	}
	
}

