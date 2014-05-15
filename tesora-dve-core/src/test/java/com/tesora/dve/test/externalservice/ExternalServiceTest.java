// OS_STATUS: public
package com.tesora.dve.test.externalservice;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.externalservice.ExternalServiceContext;
import com.tesora.dve.externalservice.ExternalServiceFactory;
import com.tesora.dve.externalservice.ExternalServicePlugin;
import com.tesora.dve.groupmanager.GroupMembershipListener.MembershipEventType;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.schema.PEExternalService;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class ExternalServiceTest extends SchemaTest {

	private static final ProjectDDL checkDDL = new PEDDL("checkdb",
			new StorageGroupDDL("check", 2, "checkg"), "schema");
	
	final static String testExternalServiceName = "foo";
	final static String testESPlugin = "com.tesora.dve.test.externalservice.TestExternalService";
	final static String testESUser = "root";
	final static String testESUsesDatastore = "true";
	final static String testESConfig = "localhost,3306,pduser,password,2,mysql-bin.000001,4";
	final static String testESConfig2 = "localhost2,33062,pduser2,password2,22,mysql-bin.0000012,42";

	PortalDBHelperConnectionResource conn;
	DBHelperConnectionResource dbh;
	
	Map<String,String> options = new HashMap<String, String>();
	
	
	@BeforeClass
	public static void setup() throws Throwable {
		projectSetup(checkDDL);
		bootHost = BootstrapHost.startServices(PETest.class);
        populateMetadata(ExternalServiceTest.class, Singletons.require(HostService.class).getProperties());
	}

	@Before
	public void connect() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
		checkDDL.create(conn);
		dbh = new DBHelperConnectionResource();
	}

	@After
	public void disconnect() throws Throwable {
		if (conn != null) {
			conn.disconnect();
			conn = null;
		}
		if (dbh != null) {
			dbh.disconnect();
			dbh = null;
		}
	}

	public ExternalServiceTest() throws SQLException, PEException {
	}
	
	@Test
	public void testExternalServiceDDL() throws Throwable {
		// test CREATE ddl
		options.put(PEExternalService.OPTION_PLUGIN, TestExternalService.class.getName());
		options.put(PEExternalService.OPTION_CONNECT_USER, testESUser);
		options.put(PEExternalService.OPTION_USE_DATASTORE, testESUsesDatastore);
		options.put(PEExternalService.OPTION_CONFIG, testESConfig);
		String esOptions = buildExternalServiceOptions(options);

		StringBuffer sql = new StringBuffer(50);
		conn.execute(sql.append("CREATE EXTERNAL SERVICE ").append(testExternalServiceName).append(" USING ").append(esOptions).toString());

		dbh.execute("USE " + TestCatalogHelper.getInstance(PETest.class).getCatalogDBName());

		verifyCreate();

		// test Stop ddl
		sql = new StringBuffer();
		sql.append("STOP EXTERNAL SERVICE ").append(testExternalServiceName);
		conn.execute(sql.toString());
		
		verifyStop();
		
		// test ALTER ddl
		options.put(PEExternalService.OPTION_PLUGIN, TestExternalService.class.getName());
		options.put(PEExternalService.OPTION_CONNECT_USER, testESUser);
		options.put(PEExternalService.OPTION_USE_DATASTORE, testESUsesDatastore);
		options.put(PEExternalService.OPTION_CONFIG, testESConfig2);
		esOptions = buildExternalServiceOptions(options);
		
		sql = new StringBuffer();
		sql.append("ALTER EXTERNAL SERVICE ").append(testExternalServiceName).append(" SET ").append(esOptions);
		conn.execute(sql.toString());

		verifyAlter();
		
		// test Start ddl
		sql = new StringBuffer();
		sql.append("START EXTERNAL SERVICE ").append(testExternalServiceName);
		conn.execute(sql.toString());
		
		verifyStart();

		conn.execute("SHOW EXTERNAL SERVICES");
		conn.execute("SHOW EXTERNAL SERVICE " + testExternalServiceName);
		
		// test DROP ddl
		
		// get datastore name first
		String datastore = null;
		ExternalService es = catalogDAO.findExternalService(testExternalServiceName);
		if (es.usesDataStore()) {
			datastore = es.getDataStoreName();
		}
		
		
		sql = new StringBuffer();
		sql.append("DROP EXTERNAL SERVICE ").append(testExternalServiceName);
		conn.execute(sql.toString());
		
		verifyDrop(datastore);
	}

	String buildExternalServiceOptions(Map<String, String> options) {
		boolean first = true;
		StringBuffer sql = new StringBuffer();
		for(String key : options.keySet()) {
			if (!first) {
				sql.append(' ');
			}
			sql.append(key).append("='").append(options.get(key)).append("'"); // NOPMD by doug on 30/11/12 3:31 PM
			first = false;
		}
		
		return sql.toString();
	}

	void verifyCreate() throws Throwable {
		dbh.assertResults("SELECT config, connect_user, uses_datastore FROM external_service WHERE name='" + testExternalServiceName + "'", 
				br(nr,testESConfig,testESUser,Boolean.TRUE));
		
		// make sure datastore has been created
		ExternalService es = catalogDAO.findExternalService(testExternalServiceName);
		if (es.usesDataStore()) {
			// if database does not exist we will throw
			conn.execute("USE " + es.getDataStoreName());
		}
			
		// make sure service is registered and started
		verifyStart();
	}
	
	void verifyAlter() throws Throwable {
		dbh.assertResults("SELECT config, connect_user, uses_datastore FROM external_service WHERE name='" + testExternalServiceName + "'", 
				br(nr,testESConfig2,testESUser,Boolean.TRUE));
	}

	void verifyDrop(String datastore) throws Throwable {
		dbh.assertResults("SELECT config, connect_user, uses_datastore FROM external_service WHERE name='" + testExternalServiceName + "'", 
				br());

		// make sure datastore has been dropped
		if (datastore != null) {
			try {
				conn.execute("USE " + datastore);
			} catch (SQLException e) {
				// expected
				String msg = e.getMessage();
				assertTrue("Expecting parse error for non existent database in use",
						StringUtils.startsWith(msg, "SchemaException: No such database:"));
			}
		}
		
		// make sure service is gone
		ExternalService es = catalogDAO.findExternalService(testExternalServiceName, false);
		assertNull("External service must not be found.", es);
		
		// make sure service is deregistered
		assertFalse(ExternalServiceFactory.isRegistered(testExternalServiceName));
	}

	void verifyStart() throws PEException {
		assertTrue("External service must be registered",ExternalServiceFactory.isRegistered(testExternalServiceName));
		ExternalServicePlugin plugin = ExternalServiceFactory.getInstance(testExternalServiceName);
		
		// make sure service is started
		assertTrue("External service must be started.", plugin.isStarted());
	}

	void verifyStop() throws PEException {
		assertTrue("External service must be registered",ExternalServiceFactory.isRegistered(testExternalServiceName));
		ExternalServicePlugin plugin = ExternalServiceFactory.getInstance(testExternalServiceName);
		
		// make sure service is started
		assertFalse("External service must be stopped.", plugin.isStarted());
	}
	
	public static class TestExternalService implements ExternalServicePlugin {
		boolean isStarted = false;
		
		@Override
		public void initialize(ExternalServiceContext ctxt) throws PEException {
		}

		@Override
		public void start() throws PEException {
			isStarted = true;
		}

		@Override
		public void stop() {
			isStarted = false;
		}

		@Override
		public boolean isStarted() {
			return isStarted;
		}

		@Override
		public String status() throws PEException {
			return null;
		}

		@Override
		public String getName() throws PEException {
			return null;
		}

		@Override
		public String getPlugin() throws PEException {
			return null;
		}

		@Override
		public void close() {
		}

		@Override
		public void reload() throws PEException {
		}
		
		@Override
		public void restart() throws PEException {
		}

		@Override
		public boolean denyServiceStart(ExternalServiceContext ctxt)
				throws PEException {
			return false;
		}
		
		@Override
		public void handleGroupMembershipEvent(MembershipEventType eventType,
				InetSocketAddress inetSocketAddress) {
		}
	}
}
