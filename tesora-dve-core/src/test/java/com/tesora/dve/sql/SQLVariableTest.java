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
				   nr,"character_set_results","latin1",
				   nr,"character_set_server","utf8"));
		conn.assertResults("show variables like 'character%'",
				br(nr,"character_set_client", "latin1",
				   nr,"character_set_connection", "latin1",
				   nr,"character_set_results","latin1",
				   nr,"character_set_server","utf8"));
		conn.execute("set @prevcharset = @@character_set_connection");
		conn.execute("set names utf8");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", "utf8",
				   nr,"character_set_connection", "utf8",
				   nr,"character_set_results","utf8",
				   nr,"character_set_server","utf8"));
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
				   nr,"character_set_results",charSet,
				   nr,"character_set_server","utf8"
				   ));
		
		charSet = "utf8";
		conn.execute("set names " + "'" + charSet + "'");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", charSet,
				   nr,"character_set_connection", charSet,
				   nr,"character_set_results",charSet,
				   nr,"character_set_server","utf8"
				   ));
		
		charSet = "ascii";
		conn.execute("set names " + "\"" + charSet + "\"");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", charSet,
				   nr,"character_set_connection", charSet,
				   nr,"character_set_results",charSet,
				   nr,"character_set_server","utf8"
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
		try {
			conn.execute("set sql_auto_is_null = 1");
			fail("should not be able to set sql_auto_is_null to 1");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: No support for sql_auto_is_null = 1 (planned)");
		}
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

		assertVariableValue(variableName, "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
		conn.execute("SET sql_mode=ALLOW_INVALID_DATES;");
		assertVariableValue(variableName, "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES");
		conn.execute("SET sql_mode=default;");
		assertVariableValue(variableName, "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
		conn.execute("SET sql_mode=\" NO_AUTO_CREATE_USER , no_engine_substitution , ALLOW_INVALID_DATES \";");
		assertVariableValue(variableName, "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES");
		conn.execute("SET sql_mode=\"\";");
		assertVariableValue(variableName, "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
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

		//		new ExpectedExceptionTester() {
		//			@Override
		//			public void test() throws Throwable {
		//				conn.execute("set session timestamp = '10'");
		//			}
		//		}.assertException(PEException.class, "Not an integral value: '10'");
		//
		//		new ExpectedExceptionTester() {
		//			@Override
		//			public void test() throws Throwable {
		//				conn.execute("set session timestamp = 'DEFAULT'");
		//			}
		//		}.assertException(PEException.class, "Not an integral value: 'DEFAULT'");
	}

	private void assertTimestampValue(final int waitTimeSec, final Long expected) throws Throwable {
		final Long value1 = Long.parseLong(getVariableValue("timestamp"));
		Thread.sleep(Long.valueOf(1000 * waitTimeSec));
		final Long value2 = Long.parseLong(getVariableValue("timestamp"));

		if (expected != null) {
			conn.assertResults("SELECT UNIX_TIMESTAMP(NOW())", br(nr, expected));
			assertEquals(expected, value1);
			assertEquals(expected, value2);
		} else {
			final Long currentSystemTime = Long.valueOf(value1 + waitTimeSec);
			conn.assertResults("SELECT UNIX_TIMESTAMP(NOW())", br(nr, currentSystemTime));
			assertEquals(currentSystemTime, value2);
		}
	}

	private void assertVariableValue(final String variableName, final Object expected) throws Throwable {
		conn.assertResults("show variables like '" + variableName + "'", br(nr, variableName, expected));
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
			nonRoot.disconnect();
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
				"set global %s = '%s'",
				"show global variables like '%s'") {

			public Object[] buildExpectedResults(String varName, String varValue) {
				return br(nr,varName,varValue);
			}
		},
		new VariableRoundTrip(
				"set @@global.%s = '%s'",
				"select variable_value from information_schema.global_variables where variable_name like '%s'")
	};

	private static final VariableRoundTrip[] sessions = new VariableRoundTrip[] {
		new VariableRoundTrip(
				"set %s = '%s'",
				"show session variables like '%s'") {
			public Object[] buildExpectedResults(String varName, String varValue) {
				return br(nr,varName,varValue);
			}
			
		},
		new VariableRoundTrip(
				"set @@session.%s = '%s'",
				"select variable_value from information_schema.session_variables where variable_name like '%s'"),
		new VariableRoundTrip(
				"set @@%s = '%s'",
				"select variable_value from information_schema.session_variables where variable_name like '%s'"),
		new VariableRoundTrip(
				"set @@local.%s = '%s'",
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
						nonRoot.execute(vrt.buildSet(handler.getName(), values.current()));
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
		conn.execute(vrt.buildSet(handler.getName(), values.next()));
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
		
		public String buildSet(String varName, String varValue) {
			return String.format(setFormat,varName,varValue);
		}
		
		public String buildQuery(String varName) {
			return String.format(queryFormat, varName);
		}
		
		public Object[] buildExpectedResults(String varName, String varValue) {
			return br(nr,varValue);
		}
		
	}
	
}

