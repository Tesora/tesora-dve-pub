package com.tesora.dve.standalone;

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.ObjectUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEBaseTest;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.comms.client.messages.ConnectRequest;
import com.tesora.dve.comms.client.messages.ConnectResponse;
import com.tesora.dve.comms.client.messages.CreateStatementRequest;
import com.tesora.dve.comms.client.messages.CreateStatementResponse;
import com.tesora.dve.comms.client.messages.ExecuteRequest;
import com.tesora.dve.comms.client.messages.ExecuteResponse;
import com.tesora.dve.comms.client.messages.FetchRequest;
import com.tesora.dve.comms.client.messages.FetchResponse;
import com.tesora.dve.errmap.ErrorCode;
import com.tesora.dve.errmap.ErrorCodeFormatter;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.errmap.InternalErrors;
import com.tesora.dve.errmap.OneParamErrorCodeFormatter;
import com.tesora.dve.errmap.TwoParamErrorCodeFormatter;
import com.tesora.dve.errmap.ZeroParamErrorCodeFormatter;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PEMappedRuntimeException;
import com.tesora.dve.lockmanager.LockManager;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.server.connectionmanager.UpdatedGlobalVariablesCallback;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.VariableManager;
import com.tesora.dve.worker.DBConnectionParameters;

/**
 * Class for tests requiring the DVE engine to be running.
 * 
 */
public class PETest extends PEBaseTest {

	protected static CatalogDAO catalogDAO = null;
	protected static BootstrapHost bootHost = null;
	// kindly leave this public - sometimes it is used for not yet committed tests
	public static Class<?> resourceRoot = PETest.class;

	private static final long NETTY_LEAK_COUNT_BASE = 0;//number of netty buffer leaks the test harness will tolerate.  If tests won't pass unless this is greater than zero, we need to track down a netty buffer leak.
    private static NettyLeakIntercept.LeakCounter leakCount;

	private static GlobalVariableState stateUndoer = null;

	// This is so that any TestNG tests will print out the class name
	// as the test is running (to mimic JUnit behavior)
	@org.testng.annotations.BeforeClass
	@Override
	public void beforeClassTestNG() {
		System.out.println("Running " + this.getClass().getName());
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
        delay("running test class","beforeClass.delay", 0L);

		applicationName = "PETest";


        //SMG:
//		System.setProperty(ResourceLeakDetector.SYSTEM_ENABLE_LEAK_DETECTION, "true");
//		System.setProperty(ResourceLeakDetector.SYSTEM_LEAK_DETECTION_INTERVAL, "1");
//		System.setProperty(ResourceLeakDetector.SYSTEM_REPORT_ALL, "true");
//
        leakCount = NettyLeakIntercept.installLeakTrap();
        System.gc();
        long leaks = leakCount.clearEmittedLeakCount();
        if (leaks != 0) {
            throw new Exception("Starting subclass of PETest, and initial buffer leak counter was non-zero ==> " + leaks + ".  Inspect slf4j output for netty leak errors, and consider re-running tests with -Dio.netty.leakDetectionLevel=PARANOID");
        }

		logger = Logger.getLogger(PETest.class);
		
		if (stateUndoer == null)
			stateUndoer = new GlobalVariableState();
	}

    private static void delay(String activity, String property, long defaultDelayMillis) {
        long delayTime = defaultDelayMillis;
        String delay = System.getProperty(property);
        if (delay != null) {
            delayTime = Long.parseLong(delay) * 1000;
        }
        try {
            if (delayTime != defaultDelayMillis)
                System.out.println("Pausing for " + delayTime + " millis before " + activity);
            Thread.sleep(delayTime);
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    @Before
	public void beforeEachTest() throws PEException {
        delay("running test case","beforeTest.delay", 0L);
	}

	@AfterClass
	public static void teardownPETest() throws Throwable {
        delay("tearing down the PE","afterClass.delay", 100L);

        if (stateUndoer != null)
        	stateUndoer.undo();
        
		if (catalogDAO != null) {
			catalogDAO.close();
			catalogDAO = null;
		}

		List<Throwable> finalThrows = new ArrayList<Throwable>();

		try {
			SSConnectionProxy.checkForLeaks();
		} catch (Throwable e) {
			// Don't throw the exception now - since we want to continue doing
			// cleanup
			finalThrows.add(e);
		}

		if (bootHost != null) {
			try {
                checkForLeakedLocks(Singletons.lookup(LockManager.class));
			} catch (Throwable e) {
				finalThrows.add(e);
			}

			// if (!bootHost.getWorkerManager().allWorkersReturned())
				// finalThrows.add(new Exception("Not all workers returned"));

			BootstrapHost.stopServices();

			bootHost = null;
		}

        System.gc();//request garbage collection run, to try and force unreferenced buffers to get collected.
        final long numOfLeaksDetected = leakCount.clearEmittedLeakCount();
        if (numOfLeaksDetected > NETTY_LEAK_COUNT_BASE) {
            finalThrows.add(new Exception("Total of '" + numOfLeaksDetected + "' Netty ByteBuf leaks detected!  Inspect slf4j output for netty leak errors, and consider re-running tests with -Dio.netty.leakDetectionLevel=PARANOID"));
        }

		if (finalThrows.size() > 0) {
			if (logger.isDebugEnabled()) {
				for (Throwable th : finalThrows) {
					logger.debug(th);
				}
			}
			throw finalThrows.get(0);
		}
	}



    private static void checkForLeakedLocks(LockManager mgr) throws Exception {
        String lockCheck = mgr.assertNoLocks();
        if (lockCheck == null)
            return;
        throw new Exception(lockCheck);
    }

    public PETest() {
		super();
		SSConnectionProxy.setOperatingContext(this.getClass().getSimpleName());
	}

	public static void populateMetadata(Class<?> testClass, Properties props) throws Exception {
		populateMetadata(testClass, props, "metadata.sql");
	}

	public static void populateMetadata(Class<?> testClass, Properties props, String sqlRes) throws Exception {
		InputStream is = testClass.getResourceAsStream(sqlRes);
		if (is != null) {
			logger.info("Reading SQL statements from " + sqlRes);
			DBHelper dbh = new DBHelper(props).connect();
			try {
				dbh.executeFromStream(is);
			} finally {
				dbh.disconnect();
			}
			is.close();
		}
	}

	public static CatalogDAO getGlobalDAO() {
		if (catalogDAO == null)
			catalogDAO = CatalogDAOFactory.newInstance();
		return catalogDAO;
	}
	
	public static void populateTemplate(DBHelper dbh, String name) throws Exception {
		dbh.executeQuery(TemplateBuilder.getClassPathCreate(name));
	}

	public static void populateTemplate(String url, String username, String password, String name) throws Exception {
		DBHelper dbh = new DBHelper(url, username, password).connect();
		try {
			dbh.executeQuery(TemplateBuilder.getClassPathCreate(name));
		} finally {
			dbh.disconnect();
		}
	}

	public static void populateSites(Class<?> testClass, Properties props) throws PEException, SQLException {
		populateSites(testClass, props, "");
	}

	public static void populateSites(Class<?> testClass, Properties props, String prefix) throws PEException,
			SQLException {
		List<PersistentSite> allSites = getGlobalDAO().findAllPersistentSites();

		for (StorageSite site : allSites) {
			String sqlRes = prefix + site.getName() + "-load.sql";
			InputStream is = testClass.getResourceAsStream(sqlRes);
			if (is != null) {
				logger.info("Reading SQL statements from " + sqlRes);
				DBHelper dbh = new DBHelper(props).connect();
				try {
					dbh.executeFromStream(is);
					dbh.disconnect();

					is.close();
				} catch (IOException e) {
					// ignore
				} finally {
					dbh.disconnect();
				}
			}
		}
	}

	public class ExecuteResult {
		public String stmtId;
		public ExecuteResponse execResp;

		public ExecuteResult(String stmtId, ExecuteResponse resp) {
			this.stmtId = stmtId;
			this.execResp = resp;
		}
	}

	public ExecuteResult executeStatement(SSConnectionProxy conProxy, DBConnectionParameters dbParams, String command)
			throws Exception {
		return executeStatement(conProxy, dbParams, command, "TestDB");
	}

	public ExecuteResult executeStatement(SSConnectionProxy conProxy, DBConnectionParameters dbParams, String command,
			String onDB) throws Exception {
		ConnectRequest conReq = new ConnectRequest(dbParams.getUserid(), dbParams.getPassword());
		ConnectResponse resp = (ConnectResponse) conProxy.executeRequest(conReq);
		assertTrue(resp.isOK());

		CreateStatementRequest csReq = new CreateStatementRequest();
		CreateStatementResponse csResp = (CreateStatementResponse) conProxy.executeRequest(csReq);
		assertTrue(csResp.isOK());
		String stmtId = csResp.getStatementId();

		ExecuteRequest execReq = new ExecuteRequest(stmtId, "use " + onDB);
		ExecuteResponse execResp = (ExecuteResponse) conProxy.executeRequest(execReq);
		assertTrue(execResp.isOK());

		execReq = new ExecuteRequest(stmtId, command);
		execResp = (ExecuteResponse) conProxy.executeRequest(execReq);
		assertTrue(execResp.isOK());

		return new ExecuteResult(stmtId, execResp);
	}

	public int showAllRows(SSConnectionProxy conProxy, DBConnectionParameters dbParams, String command)
			throws Exception {
		return showAllRows(conProxy, dbParams, command, "TestDB");
	}

	public int showAllRows(SSConnectionProxy conProxy, DBConnectionParameters dbParams, String command, String onDB)
			throws Exception {

		ExecuteResult result = executeStatement(conProxy, dbParams, command, onDB);
		String stmtId = result.stmtId;
		return showAllResultRows(conProxy, stmtId);
	}

	public int showAllResultRows(SSConnectionProxy conProxy, String stmtId) throws Exception {

		boolean moreData = false;
		int rowsPrinted = 0;
		String line = "";
		do {
			FetchRequest fetchReq = new FetchRequest(stmtId);
			FetchResponse fetchResp = (FetchResponse) conProxy.executeRequest(fetchReq);
			assertTrue(fetchResp.isOK());
			moreData = !fetchResp.noMoreData();
			if (moreData) {
				line += stmtId + ":";
				ResultChunk chunk = fetchResp.getResultChunk();
				List<ResultRow> rowList = chunk.getRowList();
				for (ResultRow row : rowList) {
					List<ResultColumn> colList = row.getRow();
					for (ResultColumn col : colList) {
						line += "\t" + col.getColumnValue().toString();
					}
					logger.debug(line);
					line = "";
					++rowsPrinted;
				}
			}
		} while (moreData);
		logger.debug("" + rowsPrinted + " rows printed"); // NOPMD by doug on 04/12/12 2:02 PM
		return rowsPrinted;
	}

	public static DBHelper buildHelper() throws Exception {
		Properties catalogProps = TestCatalogHelper.getTestCatalogProps(resourceRoot);
		Properties tempProps = (Properties) catalogProps.clone();
		tempProps.remove(DBHelper.CONN_DBNAME);
		DBHelper dbHelper = new DBHelper(tempProps);
		dbHelper.connect();
		return dbHelper;
	}

	protected static void projectSetup(StorageGroupDDL[] extras, ProjectDDL... proj) throws Exception {
		TestCatalogHelper.createTestCatalog(resourceRoot);

		DBHelper dbh = buildHelper();

		try {
			for (ProjectDDL pdl : proj) {
				for (String s : pdl.getSetupDrops())
					dbh.executeQuery(s);
				if (extras != null) {
					for (StorageGroupDDL sgl : extras) {
						for (String s : sgl.getSetupDrops(pdl.getDatabaseName()))
							dbh.executeQuery(s);
					}
				}
			}
		} finally {
			if (dbh != null)
				dbh.disconnect();
			CatalogDAOFactory.clearCache();
		}
		// TestCatalogHelper.populateMinimalCatalog(TestCatalogHelper.getCatalogDBUrl());
	}

	public static void projectSetup(ProjectDDL... proj) throws Exception {
		PETest.projectSetup(null, proj);
	}

	public static void loadSchemaIntoPE(DBHelper dbHelper, Class<?> testClass, String schemaFile, int numSites,
			String dbName) throws Exception {
		cleanupDatabase(numSites, dbName);
		try {
			dbHelper.connect();
			dbHelper.executeFromStream(PEFileUtils.getResourceStream(testClass, schemaFile));
		} finally {
			dbHelper.disconnect();
		}
	}

	public static void cleanupDatabase(int numSites, String dbName) throws Exception {
		Properties catalogProps = TestCatalogHelper.getTestCatalogProps(PETest.class);
		Properties tempProps = (Properties) catalogProps.clone();
		tempProps.remove(DBHelper.CONN_DBNAME);
		DBHelper myHelper = new DBHelper(tempProps).connect();
		try {
			for (int i = 1; i <= numSites; i++) {
				myHelper.executeQuery("DROP DATABASE IF EXISTS site" + i + "_" + dbName);
			}
		} finally {
			myHelper.disconnect();
		}
	}
	
	protected static abstract class ExpectedSqlErrorTester extends ExpectedExceptionTester {

		public <T extends PEMappedRuntimeException> void assertError(final Class<T> expectedExceptionClass, final ZeroParamErrorCodeFormatter formatter)
				throws Throwable {
			assertError(expectedExceptionClass, formatter, new Object[] {});
		}

		public <T extends PEMappedRuntimeException, P1 extends Object> void assertError(final Class<T> expectedExceptionClass,
				final OneParamErrorCodeFormatter<P1> formatter,
				final P1 first) throws Throwable {
			assertError(expectedExceptionClass, formatter, new Object[] { first });
		}

		public <T extends PEMappedRuntimeException, P1 extends Object, P2 extends Object> void assertError(final Class<T> expectedExceptionClass,
				final TwoParamErrorCodeFormatter<P1, P2> formatter,
				final P1 first, final P2 second) throws Throwable {
			assertError(expectedExceptionClass, formatter, new Object[] { first, second });
		}

		protected <T extends PEMappedRuntimeException> void assertError(final Class<T> expectedExceptionClass, final ErrorCodeFormatter formatter,
				final Object... params) throws Throwable {
			final T cause = getAssertException(expectedExceptionClass, null, false);
			assertErrorInfo(cause, formatter, params);
		}

		public <T extends SQLException> void assertSqlError(final Class<T> expectedExceptionClass, final ZeroParamErrorCodeFormatter formatter)
				throws Throwable {
			assertSqlError(expectedExceptionClass, formatter, new Object[] {});
		}

		public <T extends SQLException, P1 extends Object> void assertSqlError(final Class<T> expectedExceptionClass,
				final OneParamErrorCodeFormatter<P1> formatter,
				final P1 first) throws Throwable {
			assertSqlError(expectedExceptionClass, formatter, new Object[] { first });
		}

		public <T extends SQLException, P1 extends Object, P2 extends Object> void assertSqlError(final Class<T> expectedExceptionClass,
				final TwoParamErrorCodeFormatter<P1, P2> formatter,
				final P1 first, final P2 second) throws Throwable {
			assertSqlError(expectedExceptionClass, formatter, new Object[] { first, second });
		}

		protected <T extends SQLException> void assertSqlError(final Class<T> expectedExceptionClass, final ErrorCodeFormatter formatter,
				final Object... params) throws Throwable {
			final T cause = getAssertException(expectedExceptionClass, null, false);
			assertSqlException(cause, formatter, formatter.format(params, null));
		}

		public static <T extends SQLException> void assertSqlException(final T cause, final ErrorCodeFormatter formatter,
				final String message) throws Throwable {
			assertEquals("Should have same native code", formatter.getNativeCode(), cause.getErrorCode());
			assertEquals("Should have same sql state", formatter.getSQLState(), cause.getSQLState());
			assertEquals(message, cause.getMessage());
		}

		public static <T extends PEMappedRuntimeException> void assertErrorInfo(final T cause, final ErrorCodeFormatter formatter, final Object... params)
				throws Throwable {
			ErrorInfo ei = cause.getErrorInfo();
			assertNotNull("should have error info", ei);
			assertErrorInfo(ei, formatter, params);
		}

		protected static void assertErrorInfo(ErrorInfo info, ErrorCodeFormatter formatter, Object... params) throws Throwable {
			boolean found = false;
			for (ErrorCode ec : formatter.getHandledCodes()) {
				if (info.getCode().equals(ec)) {
					found = true;
					break;
				}
			}
			assertTrue("Should contain error code", found);
			if (formatter == InternalErrors.internalFormatter) {
				// first param is the message
				String message = (String) params[0];
				assertEquals("should have same message", formatter.format(info.getParams(), null), message);
			} else {
				assertEquals("Should have same number of parameters", params.length, info.getParams().length);
				for (int i = 0; i < params.length; i++) {
					assertEquals(String.format("should have same parameter for index %d", i), params[i], info.getParams()[i]);
				}
			}
		}

	}
	
	private static class GlobalVariableState extends UpdatedGlobalVariablesCallback {
		
		@SuppressWarnings("rawtypes")
		private final Map<VariableHandler,String> initialValues;
		private int counter;
		
		public GlobalVariableState() throws Exception {
			DBHelper helper = buildHelper();
			counter = 0;
			try {
				initialValues = buildValues(helper);
			} finally {
				helper.disconnect();
			}
			SSConnection.registerGlobalVariablesUpdater(this);
		}
		
		@SuppressWarnings("rawtypes")
		private static Map<VariableHandler,String> buildValues(DBHelper helper) throws Exception {
			ResultSet rs = null;
			HashMap<String,String> vals = new HashMap<String,String>();
			HashMap<VariableHandler,String> out = new HashMap<VariableHandler,String>();
			try {
				if (helper.executeQuery("show global variables")) {
					rs = helper.getResultSet();
					while(rs.next()) {
						vals.put(VariableManager.normalize(rs.getString(1)), rs.getString(2));
					}
				}
				for(VariableHandler vh : VariableManager.getManager().getGlobalHandlers()) {
					if (!vh.isEmulatedPassthrough()) continue;
					String vn = VariableManager.normalize(vh.getName());
					if (vh.isEmulatedPassthrough()) {
						String iv = vals.get(vn);
						out.put(vh, iv); 
					}
				}
			} finally {
				if (rs != null)
					rs.close();
			}
			return out;
		}
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void undo() throws Exception {
			if (counter == 0) return;
			DBHelper helper = buildHelper();
			try {
				Map<VariableHandler,String> cvals = buildValues(helper);
				for(VariableHandler vh : initialValues.keySet()) {
					if (!ObjectUtils.equals(initialValues.get(vh), cvals.get(vh))) {
						helper.executeQuery("set global " + vh + " = " + vh.toExternal(vh.toInternal(initialValues.get(vh))));
					}
				}
				counter = 0;
			} finally {
				helper.disconnect();
			}
		}
		
		@Override
		public void modify(String sql) {
			counter++;
//			System.out.println(sql);
		}
		
	}

}
