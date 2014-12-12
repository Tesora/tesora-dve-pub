package com.tesora.dve.sql.logfile;

import static org.junit.Assert.fail;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Before;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.PELogUtils;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.SchemaTest.TempTableChecker;
import com.tesora.dve.sql.parser.FileParser;
import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.MirrorApply;
import com.tesora.dve.sql.util.MirrorExceptionHandler;
import com.tesora.dve.sql.util.MirrorFunction;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.TestName;
import com.tesora.dve.sql.util.TestResource;

public abstract class LogFileTest extends SchemaTest implements MirrorExceptionHandler {

	public static final String JENKINS_BUILD_TESTS_PROPERTY = "jenkins.parelastic.dts.tests";
	public static final String JENKINS_BUILD_KIND_PROPERTY = "jenkins.parelastic.build.kind";

	// the log file tests by default aggregate failures and emit them in a single fail at the end.
	// for development purposes, you can collect them as they happen in a file - specify a directory
	// and the failures as they occur will be written out to <dir>/<test class name>/<test name>
	public static final String INTERMEDIATE_FAILURE_DIR = "pelogfile.progressdir";
	// likewise, if you instead want the test to fail on first failure, you can define this to turn off
	// aggregation
	public static final String FAIL_FAST = "pelogfile.failfast";
	// some tests take a long time to run, and we would like to know roughly how far through we are.
	// set this to some integer and we'll emit a message every n statements about the current statement.
	public static final String NOISY_INTERVAL = "pelogfile.periodic";
	// if set the simple name of the single test to run - used in combination with dts tests/build kind to limit
	// the tests to run
	public static final String SINGLE_TEST = "pelogfile.test";

	public enum BackingSchema {
		SINGLE, MULTI, NATIVE, SINGLE_PORTAL, MULTI_PORTAL
	}

	public enum TestPart {
		PRE, BODY, POST
	}

	public static List<TestName> runnableTests(Class<?> subtype) {
		Set<TestName> configured = getTests();
		if (configured == null) {
			// by default, this means all the tests are configured.
			configured = new HashSet<TestName>(Arrays.asList(TestName.values()));
		}
		final LogFileTestBuildKind bk = getBuildKind();
		final LogFileTestFileConfiguration config = findConfig(subtype);
		final ArrayList<TestName> runnable = new ArrayList<TestName>();
		if (config == null) {
			// no config means no can do
			return runnable;
		}
		final String tcn = System.getProperty(SINGLE_TEST);
		if ((tcn != null) && !"".equals(tcn) && !tcn.equals(subtype.getSimpleName())) {
			return runnable;
		}
		for (final TestName tn : TestName.values()) {
			if (!configured.contains(tn)) {
				continue;
			}
			final LogFileTestConfiguration p = getTestConfig(config, tn);
			if (p == null) {
				continue;
			}
			if (!p.failureReason().equals("none")) {
				continue;
			}
			boolean buildConf = false;
			for (final LogFileTestBuildKind pbk : p.builds()) {
				if (pbk == bk) {
					buildConf = true;
					break;
				}
			}
			if (!buildConf) {
				continue;
			}
			runnable.add(tn);
		}
		return runnable;
	}

	public static Set<TestName> getTests() {
		final String any = System.getProperty(JENKINS_BUILD_TESTS_PROPERTY);
		if ((any == null) || "".equals(any)) {
			return null;
		}
		final HashSet<TestName> tests = new HashSet<TestName>();
		final String[] splodey = any.trim().split(",");
		for (final String element : splodey) {
			final String item = element.trim();
			final TestName tn = TestName.getMatching(item);
			if (tn != null) {
				tests.add(tn);
			}
		}
		return tests;
	}

	private static LogFileTestBuildKind getBuildKind() {
		final String any = System.getProperty(JENKINS_BUILD_KIND_PROPERTY);
		if ((any == null) || "".equals(any)) {
			return LogFileTestBuildKind.NONE;
		}
		return LogFileTestBuildKind.fromCommandLine(any);
	}

	private static Integer getNoisyInterval() {
		final String any = System.getProperty(NOISY_INTERVAL);
		if ((any == null) || "".equals(any)) {
			return null;
		}
		try {
			return Integer.parseInt(any);
		} catch (final NumberFormatException nfe) {
			nfe.printStackTrace();
			return null;
		}
	}

	private static boolean getFailFast() {
		final String any = System.getProperty(FAIL_FAST);
		if ((any == null) || "".equals(any)) {
			return false;
		}
		return Boolean.valueOf(any);
	}

	private static File getProgressDir() {
		final String any = System.getProperty(INTERMEDIATE_FAILURE_DIR);
		if ((any == null) || "".equals(any)) {
			return null;
		}
		return new File(any);
	}

	// store the name of the test, use that for the sys prop guards
	protected FileResource file;
	protected LogFileTestFileConfiguration annoConfig = null;

	protected Integer noisyInterval = null;
	protected File failureDir = null;
	protected boolean failFast = false;
	protected LastRunConfig lastRan = null;
	protected boolean aborted = false;
	// standard compare if true compare ordered only, false compared order, then unordered if necessary and logging the compared error
	protected boolean stdCompareMode = true;
	protected File orderedCompareFailureLog = null;

	public LogFileTest(FileResource sourceFile) {
		super();
		file = sourceFile;
		noisyInterval = getNoisyInterval();
		failureDir = getProgressDir();
		failFast = getFailFast();
	}

	// ddl
	public abstract ProjectDDL getSingleSiteDDL();

	public abstract ProjectDDL getSingleSiteMTDDL();

	public abstract ProjectDDL getMultiSiteDDL();

	public abstract ProjectDDL getMultiSiteMTDDL();

	public abstract ProjectDDL getNativeDDL();

	/**
	 * @param tn
	 * @param tenant
	 * @return
	 */
	public long[] getSkipStatements(TestName tn, int tenant) {
		return null;
	}

	/**
	 * @param tn
	 * @return
	 */
	public long[] getBreakpoints(TestName tn) {
		return null;
	}

	/**
	 * @param tn
	 * @return
	 */
	public long[] ignoreResults(TestName tn) {
		return null;
	}

	/**
	 * @param tn
	 * @return
	 */
	public long[] compareUnordered(TestName tn) {
		return null;

	}

	/**
	 * @param tn
	 * @return
	 */
	public boolean verifyUpdates(TestName tn) {
		return false;
	}

	/**
	 * @param tn
	 * @return
	 */
	public boolean verifyTempTables(TestName tn) {
		return false;
	}

	public boolean isAborted() {
		return aborted;
	}

	protected Class<?> getInputResourceClass() {
		return FileParser.class;
	}

	/**
	 * @param tn
	 * @return
	 */
	protected boolean createSchema(TestName tn) {
		return true;
	}

	/**
	 * @param tn
	 * @param tenant
	 * @return
	 * @throws Throwable
	 */
	public FileResource getFile(TestName tn, int tenant) throws Throwable {
		return file;
	}

	/**
	 * @param tn
	 * @return
	 * @throws Throwable
	 */
	public boolean useThreads(TestName tn) throws Throwable {
		return false;
	}

	/**
	 * @param tn
	 * @return
	 */
	public boolean skipInitialMTLoad(TestName tn) {
		return false;
	}

	protected ProjectDDL getDDL(TestName tn, BackingSchema backingType) throws Throwable {
		if ((backingType == BackingSchema.MULTI) || (backingType == BackingSchema.MULTI_PORTAL)) {
			return (tn.isMT() ? getMultiSiteMTDDL() : getMultiSiteDDL());
		} else if ((backingType == BackingSchema.SINGLE) || (backingType == BackingSchema.SINGLE_PORTAL)) {
			return (tn.isMT() ? getSingleSiteMTDDL() : getSingleSiteDDL());
		} else if (backingType == BackingSchema.NATIVE) {
			return getNativeDDL();
		} else if (backingType == null) {
			return null;
		} else {
			throw new Throwable("Unknown backing schema type: " + backingType);
		}
	}

	protected Properties getUrlOptions() {
		final Properties props = new Properties();
		return props;
	}

	/**
	 * @param tn
	 * @param backingType
	 * @return
	 * @throws Throwable
	 */
	protected ConnectionResource getConnectionResource(TestName tn, BackingSchema backingType) throws Throwable {
		if ((backingType == BackingSchema.MULTI) || (backingType == BackingSchema.SINGLE)) {
			return new ProxyConnectionResource();
		} else if (backingType == BackingSchema.NATIVE) {
			return new DBHelperConnectionResource(getUrlOptions());
		} else if ((backingType == BackingSchema.SINGLE_PORTAL) || (backingType == BackingSchema.MULTI_PORTAL)) {
			return new PortalDBHelperConnectionResource(getUrlOptions());
		} else if (backingType == null) {
			return null;
		} else {
			throw new Throwable("Unknown backing schema type: " + backingType);
		}

	}

	protected TestResource getTestResource(TestName forTest, BackingSchema backingType) throws Throwable {
		if (backingType == null) {
			return null;
		}
		return new TestResource(getConnectionResource(forTest, backingType), getDDL(forTest, backingType));
	}

	protected TempTableChecker buildTempTableChecker(TestResource sysResource) {
		// temp tables will only exist on the temp group, see if we can get that out
		if (sysResource.getDDL().getPersistentGroup() != null) {
			return sysResource.getDDL().getPersistentGroup().buildTempTableChecker(sysResource.getDDL().getDatabaseName());
		}
		return null;
	}

	@Override
	public void onException(TestResource res, TestName currentTest, Exception e) throws Throwable {
		throw e;
	}

	// pick up info from the annotations
	private LogFileTestFileConfiguration getConfig() {
		if (annoConfig == null) {
			annoConfig = findConfig();
		}
		return annoConfig;
	}

	public static LogFileTestFileConfiguration findConfig(Class<?> onClass) {
		for (final Annotation anno : onClass.getAnnotations()) {
			if (anno.annotationType().equals(LogFileTestFileConfiguration.class)) {
				return (LogFileTestFileConfiguration) anno;
			}
		}
		return null;
	}

	private LogFileTestFileConfiguration findConfig() {
		return findConfig(this.getClass());
	}

	protected static LogFileTestConfiguration getTestConfig(LogFileTestFileConfiguration conf, TestName tn) {
		if (conf == null) {
			return null;
		}
		for (final LogFileTestConfiguration c : conf.tests()) {
			if (c.enabled().equals(tn)) {
				return c;
			}
		}
		return null;

	}

	protected LogFileTestConfiguration getTestConfig(TestName tn) {
		return getTestConfig(getConfig(), tn);
	}

	// test name, then per left and right resource:
	// null: unused
	// true: pe
	// false: native
	// the left resource always uses the single site/native ddl
	// the right resource always uses the multisite ddl
	protected void configTest(TestName tn, BackingSchema leftType, BackingSchema rightType) throws Throwable {
		aborted = false;
		final TestResource firstResource = getTestResource(tn, leftType);
		final TestResource secondResource = getTestResource(tn, rightType);
		lastRan = new LastRunConfig(tn, leftType, rightType);
		try {
			preTest(tn, firstResource, secondResource, getFile(tn, 0), 1);
			runTest(tn, firstResource, secondResource, getFile(tn, 0), createSchema(tn),
					verifyUpdates(tn), getSkipStatements(tn, 0), getBreakpoints(tn), ignoreResults(tn), compareUnordered(tn),
					verifyTempTables(tn), TestPart.BODY);
			postTest(tn, firstResource, secondResource, getFile(tn, 0), 1);
		} finally {
			maybeDisconnect(firstResource);
			maybeDisconnect(secondResource);
		}
	}

	/**
	 * @param tn
	 * @param leftResource
	 * @param rightResource
	 * @param fr
	 * @param run
	 * @throws Throwable
	 */
	protected void preTest(TestName tn, TestResource leftResource, TestResource rightResource, FileResource fr, int run) throws Throwable {
		// do what here?
	}

	/**
	 * @param tn
	 * @param leftResource
	 * @param rightResource
	 * @param fr
	 * @param run
	 * @throws Throwable
	 */
	protected void postTest(TestName tn, TestResource leftResource, TestResource rightResource, FileResource fr, int run) throws Throwable {
		// do what here?
	}

	protected static final String mtUserName = "mtdtu";
	protected static final String mtUserAccess = "localhost";
	protected static final String tenantKernel = "ten";

	protected int maxTenants(TestName tn) {
		if (tn.isMT()) {
			return 1;
		}
		return 0;
	}

	protected void configMTTest(final TestName tn, BackingSchema leftType, BackingSchema rightType) throws Throwable {
		aborted = false;
		// first off, mt tests cannot run if both backing types are pe
		int npet = 0;
		if ((leftType != null) && (leftType != BackingSchema.NATIVE)) {
			npet++;
		}
		if ((rightType != null) && (rightType != BackingSchema.NATIVE)) {
			npet++;
		}
		if (npet > 1) {
			throw new Throwable("Misconfiguration of configMTTest.  Only one side is allowed to be PE");
		}

		lastRan = new LastRunConfig(tn, leftType, rightType);

		// mt single, native mt single mirror, mt multi, native mt multi mirror

		final TestResource firstResource = getTestResource(tn, leftType);
		final TestResource secondResource = getTestResource(tn, rightType);
		try {

			TestResource rootResource = null;
			TestResource nativeResource = null;

			if (secondResource == null) {
				// either mt single or mt multi - in this case we don't need to worry about setting up the native connection
				rootResource = firstResource;
			} else if (firstResource.getDDL() != getNativeDDL()) {
				throw new Throwable("Misconfiguration of configMTTest.  Mirror test configured but lhs is not native");
			} else {
				nativeResource = firstResource;
				rootResource = secondResource;
			}

			if (useThreads(tn) && (nativeResource != null)) {
				throw new Throwable("Misconveriguration of configMTTest.  Thread use requested but native resource present");
			}

			// first up, remove any existing mt user
			removeUser(rootResource.getConnection(), mtUserName, mtUserAccess);
			// now put the system in mt mode
			// now we're going to load the database.  note that this is done as the root user.
			if (mtLoadDatabase()) {
				preTest(tn, nativeResource, rootResource, getFile(tn, 0), 1);
			}
			// now we're going to run the actual test for the root user
			MultitenantMode mm = null;
			if (rootResource.getDDL() instanceof PEDDL) {
				final PEDDL peddl = (PEDDL) rootResource.getDDL();
				mm = peddl.getMultitenantMode();
			}
			if (!skipInitialMTLoad(tn)) {
				if (nativeResource != null) {
					runTest(tn, nativeResource, rootResource, getFile(tn, 0), createSchema(tn),
							verifyUpdates(tn), getSkipStatements(tn, 0), getBreakpoints(tn), ignoreResults(tn), compareUnordered(tn),
							verifyTempTables(tn), TestPart.BODY);
				} else {
					runTest(tn, rootResource, null, getFile(tn, 0), createSchema(tn),
							verifyUpdates(tn), getSkipStatements(tn, 0), getBreakpoints(tn), ignoreResults(tn), compareUnordered(tn),
							verifyTempTables(tn), TestPart.BODY);
				}
			} else if (createSchema(tn)) {
				if (nativeResource != null) {
					runDDL(true, nativeResource);
				}
				runDDL(true, rootResource);
			}

			maybeConnect(rootResource);

			// create the second user - have to delay because we don't yet add users upon new site
			rootResource.getConnection().execute("create user '" + mtUserName + "'@'" + mtUserAccess + "' identified by '" + mtUserName + "'");

			if (useThreads(tn)) {
				final LogFileTest me = this;
				final ArrayList<Thread> threads = new ArrayList<Thread>();
				for (int i = 0; i < maxTenants(tn); i++) {
					final FileResource log = getFile(tn, i);
					if (log != null) {
						System.out.println("MT tenant " + i + " running log " + log.getFileName());
					}

					// set perms
					final String ctenant = tenantKernel + i;
					rootResource.getConnection().execute("create tenant " + ctenant + " '" + tn.toString() + " " + ctenant + "'");
					rootResource.getConnection().execute(
							"grant all on " + ctenant + ".* to '" + mtUserName + "'@'" + mtUserAccess + "' identified by '" + mtUserName + "'");

					final long[] skips = getSkipStatements(tn, i);

					final TestResource tenantResource = new TestResource(new ProxyConnectionResource(mtUserName, mtUserName), getSingleSiteDDL()
							.buildTenantDDL(ctenant));

					if (shouldCreateUserForTenant()) {
						createUserForTenant(rootResource, ctenant, ctenant);
					}

					// dup our resources
					threads.add(new Thread(ctenant) {
						@Override
						public void run() {
							try {
								if (!mtLoadDatabase()) {
									// if we didn't run the pretest earlier then
									// we want to do it for each tenant
									preTest(tn, tenantResource, null, log, 1);
								}
								runTest(tn, tenantResource, null, log, false,
										verifyUpdates(tn), skips, getBreakpoints(tn), ignoreResults(tn), compareUnordered(tn),
										verifyTempTables(tn), TestPart.BODY);
							} catch (final Throwable t) {
								if ("Aborting log file run".equals(t.getMessage())) {
									// don't overreport, just get out
									return;
								}
								System.err.println("Tenant: " + ctenant + " FAILED");
								t.printStackTrace();
								me.aborted = true;
								fail(t.getMessage());
								throw new RuntimeException(t);
							} finally {
								maybeDisconnect(tenantResource);
							}
						}
					});
				}
				for (final Thread t : threads) {
					t.start();
				}
				while (!threads.isEmpty()) {
					for (final Iterator<Thread> iter = threads.iterator(); iter.hasNext();) {
						final Thread thr = iter.next();
						try {
							thr.join();
							iter.remove();
						} catch (final InterruptedException ie) {
							// ignore
						}
					}
				}
			} else {

				for (int i = 0; i < maxTenants(tn); i++) {

					final FileResource log = getFile(tn, i);
					if (log != null) {
						System.out.println("MT tenant " + i + " running log " + log.getFileName());
					}

					// make sure everything is connected
					maybeConnect(nativeResource);
					maybeConnect(rootResource);

					if (nativeResource != null) {
						nativeResource.getConnection().execute("drop database if exists " + nativeResource.getDDL().getDatabaseName());
						nativeResource.getConnection().execute(nativeResource.getDDL().getCreateDatabaseStatement());
					}
					final String ctenant = tenantKernel + i;
					rootResource.getConnection().execute("create tenant " + ctenant + " '" + tn.toString() + " " + ctenant + "'");
					rootResource.getConnection().execute(
							"grant all on " + ctenant + ".* to '" + mtUserName + "'@'" + mtUserAccess + "' identified by '" + mtUserName + "'");
					final TestResource tenantResource = new TestResource(new ProxyConnectionResource(mtUserName, mtUserName), getSingleSiteDDL()
							.buildTenantDDL(ctenant));
					// the tenant does not need to create schema, but if we're doing native we just tossed the backing database.
					// add that in now.
					if (nativeResource != null) {
						preTest(tn, nativeResource, tenantResource, log, i + 1);
					}
					// now run again
					if (nativeResource != null) {
						runTest(tn, nativeResource, tenantResource, log, false,
								verifyUpdates(tn), getSkipStatements(tn, i), getBreakpoints(tn), ignoreResults(tn), compareUnordered(tn),
								verifyTempTables(tn), TestPart.BODY);
					} else {
						runTest(tn, tenantResource, null, log, false,
								verifyUpdates(tn), getSkipStatements(tn, i), getBreakpoints(tn), ignoreResults(tn), compareUnordered(tn),
								verifyTempTables(tn), TestPart.BODY);
					}

					maybeDisconnect(nativeResource);
					maybeDisconnect(rootResource);
					maybeDisconnect(tenantResource);
				}
			}

			// report on private/public tables
			if (getNoisyInterval() != null) {
				try {
					System.out.println("Sleeping for 2 seconds to collect adaptive garbage");
					Thread.sleep(2000);
				} catch (final InterruptedException ie) {
					// ignore
				}
				try (DBHelperConnectionResource dbh = new DBHelperConnectionResource()) {
					dbh.execute("use " + PEConstants.CATALOG);
					System.out.println("Multitenant mode: " + (mm != null ? mm.getPersistentValue() : "null MM"));
					System.out.println("number of table scopes:");
					System.out.println(dbh.printResults("select count(*) from scope"));
					System.out.println("private tables:");
					//			System.out.println(dbh.printResults("select count(*) from user_table ut, user_database ud where ut.user_database_id = ud.user_database_id and ud.multitenant_mode != 'off' and ut.privtab_ten_id is not null"));
					System.out
							.println(dbh
									.printResults("select ut.name, s.local_name, t.ext_tenant_id from user_table ut, scope s, tenant t where ut.privtab_ten_id is not null and ut.table_id = s.scope_table_id and s.scope_tenant_id = t.tenant_id"));
					System.out.println("number of shared tables:");
					System.out
							.println(dbh
									.printResults("select count(*) from user_table ut, user_database ud where ut.user_database_id = ud.user_database_id and ud.multitenant_mode != 'off' and ut.privtab_ten_id is null"));
					//			System.out.println("Compression:");
					//			System.out.println(dbh.printResults("select coalesce(local_name,ut.name), count(distinct scope_table_id), count(scope_table_id) from scope, user_table ut where scope_table_id = ut.table_id group by local_name order by local_name"));
				}
			}
		} finally {
			maybeDisconnect(firstResource);
			maybeDisconnect(secondResource);
		}

		// allow someone to do something after the test run
		postTest(tn, firstResource, secondResource, getFile(tn, 0), 1);
	}

	@Before
	public void resetDDL() throws Throwable {
		final ProjectDDL ddls[] = new ProjectDDL[] { getSingleSiteDDL(), getMultiSiteDDL(), getNativeDDL(), getSingleSiteMTDDL(), getMultiSiteMTDDL() };
		for (final ProjectDDL ddl : ddls) {
			if (ddl != null) {
				ddl.clearCreated();
			}
		}
	}

	@After
	public void teardownDDL() throws Throwable {
		if (lastRan == null) {
			return;
		}
		final List<Throwable> collectedErrors = new ArrayList<Throwable>();
		teardownDDL(lastRan.getTestName(), lastRan.getLeftType(), collectedErrors);
		teardownDDL(lastRan.getTestName(), lastRan.getRightType(), collectedErrors);
	}

	protected void teardownDDL(TestName tn, BackingSchema variety, List<Throwable> anyErrors) {
		if (variety == null) {
			return;
		}
		TestResource tr = null;
		try {
			tr = getTestResource(tn, variety);
			tr.destroy();
		} catch (final Throwable t) {
			anyErrors.add(t);
		} finally {
			if (tr != null) {
				try {
					tr.getConnection().disconnect();
				} catch (final Throwable t) {
					anyErrors.add(t);
				}
			}
		}
	}

	// config notes: if there's a native resource, it must always be the left resource

	public void nativeTest() throws Throwable {
		configTest(TestName.NATIVE, BackingSchema.NATIVE, null);
	}

	public void singleTest() throws Throwable {
		configTest(TestName.SINGLE, BackingSchema.SINGLE, null);
	}

	public void multiTest() throws Throwable {
		configTest(TestName.MULTI, BackingSchema.MULTI, null);
	}

	public void singlePortalTest() throws Throwable {
		configTest(TestName.SINGLEPORTAL, BackingSchema.SINGLE_PORTAL, null);
	}

	public void multiPortalTest() throws Throwable {
		configTest(TestName.MULTIPORTAL, BackingSchema.MULTI_PORTAL, null);
	}

	public void nativeMultiTest() throws Throwable {
		configTest(TestName.NATIVEMULTI, BackingSchema.NATIVE, BackingSchema.MULTI);
	}

	public void nativeSingleTest() throws Throwable {
		configTest(TestName.NATIVESINGLE, BackingSchema.NATIVE, BackingSchema.SINGLE);
	}

	public void nativeMultiPortalTest() throws Throwable {
		configTest(TestName.NATIVEMULTIPORTAL, BackingSchema.NATIVE, BackingSchema.MULTI_PORTAL);
	}

	public void nativeSinglePortalTest() throws Throwable {
		configTest(TestName.NATIVESINGLEPORTAL, BackingSchema.NATIVE, BackingSchema.SINGLE_PORTAL);
	}

	public void singleMTTest() throws Throwable {
		configMTTest(TestName.SINGLEMT, BackingSchema.SINGLE, null);
	}

	public void multiMTTest() throws Throwable {
		configMTTest(TestName.MULTIMT, BackingSchema.MULTI, null);
	}

	public void singleMTPortalTest() throws Throwable {
		configMTTest(TestName.SINGLEMTPORTAL, BackingSchema.SINGLE_PORTAL, null);
	}

	public void multiMTPortalTest() throws Throwable {
		configMTTest(TestName.MULTIMTPORTAL, BackingSchema.MULTI_PORTAL, null);
	}

	public void nativeSingleMTTest() throws Throwable {
		configMTTest(TestName.NATIVESINGLEMT, BackingSchema.NATIVE, BackingSchema.SINGLE);
	}

	public void nativeMultiMTTest() throws Throwable {
		configMTTest(TestName.NATIVEMULTIMT, BackingSchema.NATIVE, BackingSchema.MULTI);
	}

	public void nativeSingleMTPortalTest() throws Throwable {
		configMTTest(TestName.NATIVESINGLEMTPORTAL, BackingSchema.NATIVE, BackingSchema.SINGLE_PORTAL);
	}

	public void nativeMultiMTPortalTest() throws Throwable {
		configMTTest(TestName.NATIVEMULTIMTPORTAL, BackingSchema.NATIVE, BackingSchema.MULTI_PORTAL);
	}

	protected void runDDL(boolean create, TestResource resource) throws Throwable {
		if (create && (resource != null)) {
			resource.getDDL().create(resource);
		}
	}

	protected void maybeConnect(TestResource resource) throws Throwable {
		if (resource == null) {
			return;
		}
		if (resource.getConnection().isConnected()) {
			return;
		}
		resource.getConnection().connect();
	}

	protected static void maybeDisconnect(TestResource resource) {
		if (resource == null) {
			return;
		}
		try {
			if (!resource.getConnection().isConnected()) {
				return;
			}
			resource.getConnection().disconnect();
		} catch (final Throwable t) {
			// ignore
		}
	}

	// broke this out so that the twitter demo test can use the same framework
	protected abstract LogFileRunner buildTest(TestConfigurationParameters tc);

	protected void runTest(TestName tn, TestResource leftResource, TestResource rightResource, FileResource fr, boolean create,
			boolean doVerifyUpdates, long[] skipStatements, long[] breakpoints, long[] ignoreResults, long[] compareUnordered,
			boolean verifyTempTables, TestPart tp) throws Throwable {

		runDDL(create, leftResource);
		runDDL(create, rightResource);

		LogFileRunner ipe = null;
		try {
			PrintWriter failureLog = null;
			FileWriter failureFile = null;
			PrintWriter orderedCompareFailureLog1 = null;
			if ((tp == TestPart.BODY) && (failureDir != null)) {
				final String enclosingTestName = this.getClass().getName();
				final int lastPeriod = enclosingTestName.lastIndexOf(".");
				final int lastDollar = enclosingTestName.lastIndexOf("$");
				final String testDir = enclosingTestName.substring((lastPeriod > lastDollar ? lastPeriod : lastDollar) + 1);
				final File testDirFile = new File(failureDir, testDir);
				if (!testDirFile.exists()) {
					testDirFile.mkdir();
				}
				failureFile = new FileWriter(new File(testDirFile, tn.getNewName()));
				failureLog = new PrintWriter(failureFile);
				failureLog.println("Failures for " + enclosingTestName + "/" + tn.getNewName() + "; build " + PELogUtils.getBuildVersionString(false));
				failureLog.flush();
				if (!stdCompareMode) {
					orderedCompareFailureLog1 = new PrintWriter(new FileWriter(new File(testDirFile, tn.getNewName() + "_ordered_compare_failure")));
				}
			}
			maybeConnect(leftResource);
			maybeConnect(rightResource);
			final TestConfigurationParameters tc = new TestConfigurationParameters(
					tn, leftResource, rightResource, fr, doVerifyUpdates, skipStatements, breakpoints, ignoreResults, compareUnordered,
					failureLog, failFast, noisyInterval, tp, orderedCompareFailureLog1);
			ipe = buildTest(tc);
			ipe.runFile();
			if (failureLog != null) {
				failureLog.close();
			}
			if (failureFile != null) {
				failureFile.close();
			}
			final String anyDelayedFailures = ipe.getDelayedFailures();
			if (anyDelayedFailures != null) {
				fail(anyDelayedFailures);
			}

			if (verifyTempTables) {
				TestResource sysResource = null;
				if ((leftResource != null) && (leftResource.getDDL() == getMultiSiteDDL())) {
					sysResource = leftResource;
				} else if ((rightResource != null) && (rightResource.getDDL() == getMultiSiteDDL())) {
					sysResource = rightResource;
				}
				if (sysResource != null) {
					final TempTableChecker ttc = buildTempTableChecker(sysResource);
					if (ttc != null) {
						try {
							final String any = ttc.check();
							if (any != null) {
								fail("Found at least one temp table: " + any);
							}
						} finally {
							ttc.close();
						}
					}
				}
			}
		} finally {
			if (ipe != null) {
				ipe.close();
			}
		}

	}

	public boolean mtLoadDatabase() {
		return true;
	}

	// TODO make this protected
	public boolean shouldCreateUserForTenant() {
		return false;
	}

	// TODO make this protected
	public void createUserForTenant(TestResource rootResource, String tenant, String password) throws Throwable {
		removeUser(rootResource.getConnection(), tenant, mtUserAccess);
		rootResource.getConnection().execute("create user '" + tenant + "'@'" + mtUserAccess + "' identified by '" + password + "'");
		rootResource.getConnection().execute("grant all on " + tenant + ".* to '" + tenant + "'@'" + mtUserAccess + "' identified by '" + password + "'");
	}

	public void setStdCompareMode(boolean mode) {
		stdCompareMode = mode;
	}

	private static class ConnectedResources {

		private final TestResource root;
		private final HashMap<Integer, TestResource> all;

		public ConnectedResources(TestResource root) {
			this.root = root;
			this.all = new HashMap<Integer, TestResource>();
		}

		public void disconnect(Integer id) throws Throwable {
			if (id == null) {
				return;
			}
			final TestResource candidate = all.get(id);
			if (candidate == null) {
				return;
			}
			candidate.getConnection().disconnect();
			all.remove(id);
		}

		public void connect(Integer id) throws Throwable {
			if (id == null) {
				return;
			}
			getResource(id);
		}

		public TestResource getResource(Integer id) throws Throwable {
			TestResource candidate = all.get(id);
			if (candidate == null) {
				final ConnectionResource cr = root.getConnection().getNewConnection();
				candidate = new TestResource(cr, root.getDDL(), id);
				all.put(id, candidate);
				candidate.getConnection().execute("use " + candidate.getDDL().getDatabaseName());
			}
			return candidate;
		}

		public void close() {
			for (final TestResource tr : all.values()) {
				try {
					tr.getConnection().disconnect();
				} catch (final Throwable t) {
					// ignore
				}
			}
			all.clear();
		}

	}

	protected static class MirrorInvoker extends ParserInvoker {

		private ConnectedResources leftResources;
		private ConnectedResources rightResources;
		private final TestResource rightResource;
		private final TestResource leftResource;

		private final HashMap<Integer, Boolean> connectedResources;
		private boolean connected;

		private final HashSet<Long> skips;
		private final HashSet<Long> bps;
		private final HashSet<Long> execOnly;
		private final HashSet<Long> unordered;
		private final boolean verifyUpdates;
		private final LogFileTest parent;
		private final TestName currentTest;

		private final List<Throwable> failures;
		private final PrintWriter failureLog;
		private final boolean stopOnFirstFailure;
		private final Integer echoInterval;
		private final PrintWriter orderedCompareFailureLog;

		public MirrorInvoker(TestResource left, TestResource right, LogFileTest parentTest, TestName runningTest, boolean verify, long[] skipStatements,
				long[] breakpoints, long[] ignoreResults, long[] compareUnordered,
				PrintWriter logFailures, Integer gechoInterval,
				boolean failFast, PrintWriter logOrderedCompareFailures) {
			super(ParserOptions.NONE);
			leftResource = left;
			rightResource = right;
			if (left != null) {
				leftResources = new ConnectedResources(leftResource);
			}
			if (right != null) {
				rightResources = new ConnectedResources(rightResource);
			}
			connectedResources = new HashMap<Integer, Boolean>();
			verifyUpdates = verify;
			skips = toSet(skipStatements);
			bps = toSet(breakpoints);
			execOnly = toSet(ignoreResults);
			unordered = toSet(compareUnordered);
			parent = parentTest;
			currentTest = runningTest;
			failures = new ArrayList<Throwable>();
			failureLog = logFailures;
			stopOnFirstFailure = failFast;
			echoInterval = gechoInterval;
			orderedCompareFailureLog = logOrderedCompareFailures;
			// we start out connected
			connected = true;
		}

		public void close() {
			if (leftResources != null) {
				leftResources.close();
			}
			if (rightResources != null) {
				rightResources.close();
			}
			if (leftResource != null) {
				try {
					leftResource.getConnection().disconnect();
				} catch (final Throwable t) {
					// ignore
				}
			}
			if (rightResource != null) {
				try {
					rightResource.getConnection().disconnect();
				} catch (final Throwable t) {
					// ignore
				}
			}
		}

		public String getDelayedFailures() throws Throwable {
			if (failures.size() > 0) {
				// we're going to put all of the messages in one big message, and emit that instead.
				// this means we have to create all the stack traces too, what fun!
				final StringWriter writer = new StringWriter();
				final PrintWriter pw = new PrintWriter(writer);
				pw.println("All failures:");
				for (final Throwable t : failures) {
					t.printStackTrace(pw);
					pw.println("  ---------------------  ");
				}
				pw.flush();
				pw.close();
				writer.close();
				return writer.getBuffer().toString();
			}
			return null;
		}

		private static HashSet<Long> toSet(long[] in) {
			final HashSet<Long> out = new HashSet<Long>();
			if (in == null) {
				return out;
			}
			for (final long l : in) {
				out.add(new Long(l));
			}
			return out;
		}

		private void doDisconnect(Integer id) throws Throwable {
			if (id == null) {
				// do root
				if (connected) {
					connected = false;
					if (leftResource != null) {
						leftResource.getConnection().disconnect();
					}
					if (rightResource != null) {
						rightResource.getConnection().disconnect();
					}
				}
				return;
			}
			final Boolean isConnected = connectedResources.get(id);
			if (isConnected == null) {
				return;
			}
			if (Boolean.FALSE.equals(isConnected)) {
				return;
			}
			connectedResources.put(id, Boolean.FALSE);
			if (leftResources != null) {
				leftResources.disconnect(id);
			}
			if (rightResources != null) {
				rightResources.disconnect(id);
			}
		}

		private void connectResource(TestResource tr) throws Throwable {
			if (tr == null) {
				return;
			}
			tr.getConnection().connect();
			tr.getConnection().execute("use " + tr.getDDL().getDatabaseName());
		}

		private void doConnect(Integer id) throws Throwable {
			if (id == null) {
				// do root
				if (!connected) {
					connected = true;
					connectResource(leftResource);
					connectResource(rightResource);
				}
			} else {
				final Boolean isConnected = connectedResources.get(id);
				if (Boolean.TRUE.equals(isConnected)) {
					return;
				}
				connectedResources.put(id, Boolean.TRUE);
				if (leftResources != null) {
					leftResources.connect(id);
				}
				if (rightResources != null) {
					rightResources.connect(id);
				}
			}
		}

		@Override
		public String parseOneLine(LineInfo info, String line) throws Throwable {
			if (parent.isAborted()) {
				throw new Throwable("Aborting log file run");
			}
			final Long oli = new Long(info.getLineNumber());
			if (skips.contains(oli)) {
				return line;
			}
			if (bps.contains(oli)) {
				System.out.println("Starting statement: " + oli);
			}
			TaggedLineInfo tli = null;
			if (info instanceof TaggedLineInfo) {
				tli = (TaggedLineInfo) info;
			}
			Integer connID = null;
			LineTag lt = null;
			if (tli != null) {
				connID = (tli.getConnectionID() == -1 ? null : tli.getConnectionID());
				lt = tli.getTag();
			}
			if ((echoInterval != null) && ((oli % echoInterval.longValue()) == 0)) {
				System.out.println("starting stmt " + oli + ": '" + line + "'");
			}
			if (LineTag.CONNECT.equals(lt)) {
				doConnect(connID);
			} else if (LineTag.DISCONNECT.equals(lt)) {
				doDisconnect(connID);
			} else {
				final boolean justExec = execOnly.contains(oli);
				TestResource lr = null;
				TestResource rr = null;
				if (connID == null) {
					lr = leftResource;
					rr = rightResource;
				} else {
					lr = (leftResources == null ? null : leftResources.getResource(connID));
					rr = (rightResources == null ? null : rightResources.getResource(connID));
				}
				try {
					if (orderedCompareFailureLog == null) {
						if (LineTag.SELECT_ORDERED.equals(lt)) {
							if (unordered.contains(oli)) {
								lt = LineTag.SELECT;
							}
						}
						if (LineTag.SELECT_ORDERED.equals(lt)) {
							new MirrorFunction(info, line, parent, currentTest, false, justExec).execute(lr, rr);
						} else if (LineTag.SELECT.equals(lt)) {
							new MirrorFunction(info, line, parent, currentTest, true, justExec).execute(lr, rr);
						} else {
							// boolean isTruncate = line.toLowerCase().trim().indexOf("truncate") > - 1;
							new MirrorApply(info, line, parent, currentTest, justExec/*
																					 * ||
																					 * isTruncate
																					 */).execute(lr, rr);
							if (verifyUpdates && (lr != null) && (rr != null) && (tli != null) && (tli.getTable() != null) && !justExec) {
								final String verstmt = "select * from " + tli.getTable();
								new MirrorFunction(info, verstmt, parent, currentTest, true, false).execute(lr, rr);
							}
						}
					} else {
						boolean tryUnordered = false;
						Throwable orderedThrowable = null;
						if (LineTag.SELECT_ORDERED.equals(lt)) {
							try {
								new MirrorFunction(info, line, parent, currentTest, false, justExec).execute(lr, rr);
							} catch (final Throwable t) {
								orderedThrowable = t;
							}
							lt = LineTag.SELECT;
							tryUnordered = true;
						}

						if (tryUnordered && LineTag.SELECT.equals(lt)) {
							new MirrorFunction(info, line, parent, currentTest, true, justExec).execute(lr, rr);
							// if we get here then maybe log the ordered exception
							if ((orderedThrowable != null) && (orderedThrowable instanceof AssertionError) &&
									(orderedCompareFailureLog != null)) {
								orderedThrowable.printStackTrace(orderedCompareFailureLog);
								orderedCompareFailureLog.println(" ");
								orderedCompareFailureLog.flush();
							}
						} else if (!(LineTag.SELECT.equals(lt) || LineTag.SELECT_ORDERED.equals(lt))) {
							// boolean isTruncate = line.toLowerCase().trim().indexOf("truncate") > - 1;
							new MirrorApply(info, line, parent, currentTest, justExec/*
																					 * ||
																					 * isTruncate
																					 */).execute(lr, rr);
							if (verifyUpdates && (lr != null) && (rr != null) && (tli != null) && (tli.getTable() != null) && !justExec) {
								final String verstmt = "select * from " + tli.getTable();
								new MirrorFunction(info, verstmt, parent, currentTest, true, false).execute(lr, rr);
							}
						}
					}
				} catch (Throwable t) {
					t.printStackTrace();
					// we're going to try to get the plan via explain and add that in to the message.
					if (!LineTag.DDL.equals(lt)) {
						t = annotateFailureWithPlan(t, line, lr, rr, lt);
					}
					if (stopOnFirstFailure) {
						throw t;
					}
					failures.add(t);
					if (failureLog != null) {
						t.printStackTrace(failureLog);
						failureLog.flush();
					}
				}
			}

			return line;
		}
	}

	public static class FileResource {

		public static FileResource buildFromFile(final File target) {
			final String filename = target.getAbsolutePath();
			return new FileResource(FilenameUtils.getFullPath(filename), FilenameUtils.getName(filename));
		}

		protected Class<?> relativeToClass;
		protected String relativeToDirectory;
		protected String fileName;

		private FileResource(Class<?> relClass, String relDir, String fn) {
			relativeToClass = relClass;
			relativeToDirectory = relDir;
			fileName = fn;
		}

		public FileResource(Class<?> relClass, String fn) {
			this(relClass, null, fn);
		}

		public FileResource(String relDir, String fn) {
			this(null, relDir, fn);
		}

		public InputStream getFileInput() throws Throwable {
			if (relativeToClass != null) {
				return PEFileUtils.getResourceStream(relativeToClass, fileName);
			} else if (relativeToDirectory != null) {
				return new FileInputStream(new File(new File(relativeToDirectory), fileName));
			} else {
				throw new Throwable("Incorreclty configured file resource");
			}
		}

		public FileResource adapt(String newfn) {
			if (relativeToClass != null) {
				return new FileResource(relativeToClass, newfn);
			}
			return new FileResource(relativeToDirectory, newfn);
		}

		public String getFileName() {
			return fileName;
		}
	}

	public static class TestConfigurationParameters {

		protected TestName tn;
		protected TestResource leftResource;
		protected TestResource rightResource;
		protected FileResource fileToRun;
		protected boolean verifyUpdates;
		protected long[] skipStatements;
		protected long[] breakpoints;
		protected long[] ignoreResults;
		protected long[] compareUnordered;
		protected PrintWriter failureLog;
		protected boolean failFast;
		protected TestPart part;
		protected Integer noisyInterval;
		protected PrintWriter orderedCompareFailureLog;

		public TestConfigurationParameters(TestName gtn, TestResource gleftResource, TestResource gRightResource, FileResource gfr,
				boolean gverifyUpdates, long[] gskips, long[] gbreaks, long[] gignores, long[] gcompUnordered, PrintWriter gFailureLog,
				boolean gFailFast, Integer echoInterval, TestPart tp, PrintWriter logOrderedCompareFailure) {
			tn = gtn;
			leftResource = gleftResource;
			rightResource = gRightResource;
			fileToRun = gfr;
			verifyUpdates = gverifyUpdates;
			skipStatements = gskips;
			breakpoints = gbreaks;
			ignoreResults = gignores;
			compareUnordered = gcompUnordered;
			failureLog = gFailureLog;
			failFast = gFailFast;
			part = tp;
			noisyInterval = echoInterval;
			orderedCompareFailureLog = logOrderedCompareFailure;
		}

		public TestName getTestName() {
			return tn;
		}

		public TestResource getLeftResource() {
			return leftResource;
		}

		public TestResource getRightResource() {
			return rightResource;
		}

		public FileResource getFileToRun() {
			return fileToRun;
		}

		public boolean isVerifyUpdates() {
			return verifyUpdates;
		}

		public long[] getSkipStatements() {
			return skipStatements;
		}

		public long[] getBreakpoints() {
			return breakpoints;
		}

		public long[] getIgnoreResults() {
			return ignoreResults;
		}

		public long[] getCompareUnordered() {
			return compareUnordered;
		}

		public PrintWriter getFailureLog() {
			return failureLog;
		}

		public boolean isFailFast() {
			return failFast;
		}

		public TestPart getPart() {
			return part;
		}

		public Integer getEchoInterval() {
			return noisyInterval;
		}

		public PrintWriter getOrderedCompareFailureLog() {
			return orderedCompareFailureLog;
		}
	}

	protected static class PELogFileRunner extends AbstractRunner {

		public PELogFileRunner(TestConfigurationParameters tc, LogFileTest parent) {
			super(tc, parent);
		}

		protected PELogFileRunner(TestConfigurationParameters tc, MirrorInvoker mi) {
			super(tc, mi);
		}

		@Override
		public void runFile() throws Throwable {
			new FileParser().parseOneFilePELog(tc.getFileToRun().getFileInput(), mi);
		}
	}

	protected static class NativeLogFileRunner extends AbstractRunner {

		public NativeLogFileRunner(TestConfigurationParameters tc, LogFileTest parent) {
			super(tc, parent);
		}

		protected NativeLogFileRunner(TestConfigurationParameters tc, MirrorInvoker mi) {
			super(tc, mi);
		}

		@Override
		public void runFile() throws Throwable {
			new FileParser().parseOneMysqlLogFile(tc.getFileToRun().getFileInput(), mi, CharsetUtil.UTF_8);
		}
	}

	// this is the default runner thingy
	protected static abstract class AbstractRunner implements LogFileRunner {

		protected MirrorInvoker mi;
		protected TestConfigurationParameters tc;

		public AbstractRunner(TestConfigurationParameters tc, LogFileTest parent) {
			this(tc, new MirrorInvoker(tc.getLeftResource(), tc.getRightResource(), parent,
					tc.getTestName(), tc.isVerifyUpdates(),
					tc.getSkipStatements(), tc.getBreakpoints(),
					tc.getIgnoreResults(), tc.getCompareUnordered(),
					tc.getFailureLog(), tc.getEchoInterval(), tc.isFailFast(),
					tc.getOrderedCompareFailureLog()));
		}

		protected AbstractRunner(TestConfigurationParameters tc, MirrorInvoker mi) {
			this.tc = tc;
			this.mi = mi;
		}

		@Override
		public String getDelayedFailures() throws Throwable {
			return mi.getDelayedFailures();
		}

		@Override
		public void close() throws Throwable {
			mi.close();
		}

	}

	protected static class LastRunConfig {

		protected TestName tn;
		protected BackingSchema leftType;
		protected BackingSchema rightType;

		public LastRunConfig(TestName tn, BackingSchema lt, BackingSchema rt) {
			this.tn = tn;
			leftType = lt;
			rightType = rt;
		}

		public BackingSchema getLeftType() {
			return leftType;
		}

		public BackingSchema getRightType() {
			return rightType;
		}

		public TestName getTestName() {
			return tn;
		}
	}

}
