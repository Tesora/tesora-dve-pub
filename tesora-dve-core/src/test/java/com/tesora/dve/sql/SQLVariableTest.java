// OS_STATUS: public
package com.tesora.dve.sql;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ProxyConnectionResourceResponse;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class SQLVariableTest extends SchemaTest {

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",2,"sysg"),
				"schema");

	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(sysDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		sysDDL.create(pcr);
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
				   nr,"character_set_results","latin1"));
		conn.assertResults("show variables like 'character%'",
				br(nr,"character_set_client", "latin1",
				   nr,"character_set_connection", "latin1",
				   nr,"character_set_results","latin1"));
		conn.execute("set @prevcharset = @@character_set_connection");
		conn.execute("set names utf8");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", "utf8",
				   nr,"character_set_connection", "utf8",
				   nr,"character_set_results","utf8"));
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
		unfiltered.assertEqualResults("testShowVariablesFilter", true, false, filtered);
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
				   nr,"character_set_results",charSet));
		
		charSet = "utf8";
		conn.execute("set names " + "'" + charSet + "'");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", charSet,
				   nr,"character_set_connection", charSet,
				   nr,"character_set_results",charSet));
		
		charSet = "ascii";
		conn.execute("set names " + "\"" + charSet + "\"");
		conn.assertResults("show session variables like 'character%'",
				br(nr,"character_set_client", charSet,
				   nr,"character_set_connection", charSet,
				   nr,"character_set_results",charSet));
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
		} catch (PEException re) {
			SchemaTest.assertSchemaException(re, "No support for sql_auto_is_null = 1 (planned)");
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

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				conn.execute("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE");
			}
		}.assertException(PESQLException.class, "Unable to build plan - No support for native global variables");
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

		// new ExpectedExceptionTester() {
		// @Override
		// public void test() throws Throwable {
		// conn.execute("SET sql_mode=NON_EXISTING_MODE_PE336;");
		// }
		// }.assertException(PEException.class);
	}

	private void assertVariableValue(final String variableName, final Object expected) throws Throwable {
		conn.assertResults("show variables like '" + variableName + "'", br(nr, variableName, expected));
	}
}
