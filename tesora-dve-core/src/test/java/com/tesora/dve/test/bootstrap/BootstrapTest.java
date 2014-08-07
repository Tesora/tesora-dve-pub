package com.tesora.dve.test.bootstrap;

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
import static org.junit.Assert.assertTrue;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.*;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.Project;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.distribution.PELockedException;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepAddGenerationOperation;
import com.tesora.dve.queryplan.QueryStepDDLOperation;
import com.tesora.dve.queryplan.QueryStepInsertByKeyOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.queryplan.QueryStepSelectByKeyOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.worker.MysqlTextResultChunkProvider;
import com.tesora.dve.worker.UserCredentials;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class BootstrapTest extends PETest {
	
	private final static String SITE1_NAME = "site1"; 
	private final static String SITE2_NAME = "site2"; 
	private final static String SITE3_NAME = "site3"; 
	private final static String SG_NAME = "sg"; 
	private final static String DB_NAME = "db"; 
	private final static String PROJ_NAME = "proj"; 
	private final static String SITE_URL = "jdbc:mysql:///";
	private final static String SITE_USER = "root";
	private final static String SITE_PASSWORD = "password";
	private static final String TABLE_NAME = "foo";
	private static final String TABLE2_NAME = "foobar";
	private static final String RANGE_NAME = "test_range";

	public Logger logger = Logger.getLogger(BootstrapTest.class);

	static SSConnection ssConnection;
	
	static int testId = 0;
	static boolean suppressCachePurge;

	@BeforeClass
	public static void setup() throws Exception {
		suppressCachePurge = Boolean.getBoolean(PersistentGroup.PERSISTENT_GROUP_SUPPRESS_CACHE_PURGE);
		System.setProperty(PersistentGroup.PERSISTENT_GROUP_SUPPRESS_CACHE_PURGE, "true");
		
		Class<?> bootClass = PETest.class;
		TestCatalogHelper.createTestCatalog(bootClass,3);

		bootHost = BootstrapHost.startServices(bootClass);
		
		catalogDAO = CatalogDAOFactory.newInstance();
        populateSites(BootstrapTest.class, Singletons.require(HostService.class).getProperties());
		catalogDAO.close();
		catalogDAO = null;
	}

	@AfterClass
	public static void shutdown() throws Exception {
		System.setProperty(PersistentGroup.PERSISTENT_GROUP_SUPPRESS_CACHE_PURGE, Boolean.toString(suppressCachePurge));
	}
	
	@Override
	@Before
	public void beforeEachTest() throws PEException {
		++testId;
		catalogDAO = CatalogDAOFactory.newInstance();		
		ssConnection = new SSConnection();
	
		SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
		ssConnection.startConnection(new UserCredentials(bootHost.getProperties()));
		ssConnection.setPersistentDatabase(catalogDAO.findDatabase("TestDB"));
	}
	
	@After
	public void afterEachTest() throws Exception {
		SSConnectionAccessor.setCatalogDAO(ssConnection, null);
		catalogDAO.close();
		catalogDAO = null;
		ssConnection.close();
		ssConnection = null;
	}
	
	public String nameForTest(String n) {
		return n + "_" + testId;
	}
	
	@Test
	public void createStorageSite() throws Throwable {
		final String siteName = nameForTest(SITE1_NAME);
		
		utilCreateStorageSite(siteName, SITE_URL, SITE_USER, SITE_PASSWORD);

		PersistentSite ss = catalogDAO.findPersistentSite(siteName);
		assertTrue(ss.getName().equals(siteName));
		assertTrue(ss.getMasterUrl().equals(SITE_URL));
	}

	private PersistentSite utilCreateStorageSite(final String siteName, final String url, final String user, final String password) throws Throwable {
		return (PersistentSite) 
				catalogDAO.new EntityGenerator() {
					@Override
					public CatalogEntity generate() throws PEException {
						return catalogDAO.createPersistentSite(siteName, url, user, password);
					}
				}.execute();
	}
	
	private DistributionRange utilCreateDistRange(final String rangeName, final PersistentGroup sg, final String types ) throws Throwable {
		return (DistributionRange)
				catalogDAO.new EntityGenerator() {
					@Override
					public CatalogEntity generate() {
						return new DistributionRange(rangeName, sg, types);
			}
		}.execute();
	}

	@Test
	public void createEmptyStorageGroup() throws Throwable {
		final String sgName = nameForTest(SG_NAME);
		
		PersistentGroup sg = utilCreateStorageGroup(sgName, Collections.<PersistentSite>emptyList());
		assertTrue(sg.getName().equals(sgName));
		assertEquals(0, sg.getStorageSites().size());
		assertEquals(1, sg.getGenerations().size());
		
		catalogDAO.refresh(sg);
		assertTrue(sg.getName().equals(sgName));
		assertEquals(0, sg.getStorageSites().size());
		assertEquals(1, sg.getGenerations().size());
	}
	
	@Test
	public void createStorageGroupOneSite() throws Throwable {
		final String sgName = nameForTest(SG_NAME);
		final String siteName1 = nameForTest(SITE1_NAME);
		
		PersistentSite ss = utilCreateStorageSite(siteName1, SITE_URL, SITE_USER, SITE_PASSWORD);
		PersistentGroup sg = utilCreateStorageGroup(sgName, Arrays.asList(new PersistentSite[] { ss }));
		
		List<PersistentSite> allSites = sg.getStorageSites();
		assertTrue(allSites != null);
		assertEquals(1, allSites.size());
		assertTrue(siteName1.equals(allSites.get(0).getName()));
		
		catalogDAO.refresh(sg);
		allSites = sg.getStorageSites();
		assertTrue(allSites != null);
		assertEquals(1, allSites.size());
		assertTrue(siteName1.equals(allSites.get(0).getName()));
	}

	@Test
	public void createStorageGroupTwoSite() throws Throwable {
		final String sgName = nameForTest(SG_NAME);
		final String siteName1 = nameForTest(SITE1_NAME);
		final String siteName2 = nameForTest(SITE2_NAME);
		
		PersistentSite ss1 = utilCreateStorageSite(siteName1, SITE_URL, SITE_USER, SITE_PASSWORD);
		PersistentSite ss2 = utilCreateStorageSite(siteName2, SITE_URL, SITE_USER, SITE_PASSWORD);
		PersistentGroup sg = utilCreateStorageGroup(sgName, Arrays.asList(new PersistentSite[] { ss1, ss2 }));
		
		List<PersistentSite> allSites = sg.getStorageSites();
		assertTrue(allSites != null);
		assertEquals(2, allSites.size());
		assertTrue(siteName1.equals(allSites.get(0).getName()));
		assertTrue(siteName2.equals(allSites.get(1).getName()));
		
		catalogDAO.refresh(sg);
		allSites = sg.getStorageSites();
		assertTrue(allSites != null);
		assertEquals(2, allSites.size());
		assertTrue(siteName1.equals(allSites.get(0).getName()));
		assertTrue(siteName2.equals(allSites.get(1).getName()));
	}

	private PersistentGroup utilCreateStorageGroup(final String sgName, final List<PersistentSite> sites) throws Throwable {
		return (PersistentGroup) 
				catalogDAO.new EntityGenerator() {
					@Override
					public CatalogEntity generate() throws PELockedException {
						PersistentGroup sg = new PersistentGroup(sgName);
						sg.addAllSites(sites);
						return sg;
					}
				}.execute();
	}
	
	@Test
	public void createProject() throws Throwable {
		final String projName = nameForTest(PROJ_NAME);
		final String sgName = nameForTest(SG_NAME);
		final String siteName1 = nameForTest(SITE1_NAME);
		final String siteName2 = nameForTest(SITE2_NAME);
		
		PersistentSite ss1 = utilCreateStorageSite(siteName1, SITE_URL, SITE_USER, SITE_PASSWORD);
		PersistentSite ss2 = utilCreateStorageSite(siteName2, SITE_URL, SITE_USER, SITE_PASSWORD);
		final PersistentGroup sg = utilCreateStorageGroup(sgName, Arrays.asList(new PersistentSite[] { ss1, ss2 }));
		
		final Project p = utilCreateProject(projName);
		
		assertTrue(p.getName().equals(projName));
		catalogDAO.refresh(p);
		assertTrue(p.getName().equals(projName));

		/*
		
		catalogDAO.new EntityUpdater() {
			@Override
			public CatalogEntity update() {
				p.setDefaultStorageGroup(sg);
				return p;
			}
		}.execute();
		
		assertTrue(p.getDefaultStorageGroup().equals(sg));
		catalogDAO.refresh(p);
		assertTrue(p.getDefaultStorageGroup().equals(sg));
		*/
	}

	private Project utilCreateProject(final String projName) throws Throwable {
		return (Project) 
				catalogDAO.new EntityGenerator() {
					@Override
					public CatalogEntity generate() {
						return new Project(projName);
					}
				}.execute();
	}

	@Test
	public void createDatabase() throws Throwable {
		final String siteName = nameForTest(SITE1_NAME);
		final String sgName = nameForTest(SG_NAME);
		final String dbName = nameForTest(DB_NAME);
		
		final PersistentSite ss = catalogDAO.createPersistentSite(siteName, SITE_URL, SITE_USER, SITE_PASSWORD);
		final PersistentGroup sg = new PersistentGroup(sgName);

		UserDatabase db = (UserDatabase)
				catalogDAO.new EntityGenerator() {
			@Override
			public CatalogEntity generate() throws Throwable {
				catalogDAO.persistToCatalog(ss);
				catalogDAO.persistToCatalog(sg);
				sg.addAllSites(Arrays.asList(new PersistentSite[] { ss }));
				UserDatabase db = new UserDatabase(dbName, sg);
				return db;
			}
		}.execute();
		
		assertTrue(dbName.equals(db.getName()));
		assertEquals(sg, db.getDefaultStorageGroup());
		// assertEquals(p, db.getProject());
		catalogDAO.refresh(db);
		assertTrue(dbName.equals(db.getName()));
		assertEquals(sg, db.getDefaultStorageGroup());
		// assertEquals(p, db.getProject());
		assertEquals(UserDatabase.getNameOnSite(dbName, ss), db.getNameOnSite(ss));
	}
	
	public void addGenToTable(DistributionModel dm, PersistentSite[] gen1, PersistentSite[] gen2, 
			int expectedCount, boolean testLastGenOnly) throws Throwable {
		final String sgName = nameForTest(SG_NAME);
		final String tableName = nameForTest(TABLE_NAME);
		
		UserDatabase db = ssConnection.getPersistentDatabase();

		PersistentGroup sg = utilCreateStorageGroup(sgName, Arrays.asList(gen1));

		UserTable foo = utilCreateTableTwoColumns(tableName, sg, dm, db);
		QueryStepDDLOperation createTableDDL =
				new QueryStepDDLOperation(db, new SQLCommand(foo.getCreateTableStmt()), null);
		createTableDDL.addCatalogUpdate(foo);

		WorkerGroup wg = WorkerGroupFactory.newInstance(ssConnection, sg, db);
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		createTableDDL.execute(ssConnection, wg, results);

		try {
			assertEquals(0, results.getUpdateCount());

			for (int i = 0; i < 10; ++i) {
				results = new MysqlTextResultChunkProvider();
				utilInsertTableTwoColumns(i, foo, wg, db, results);
				assertEquals(1, results.getUpdateCount());
			}

			QueryStepOperation qso = new QueryStepAddGenerationOperation(sg, Arrays.asList(gen2),
					CacheInvalidationRecord.GLOBAL);
			results = new MysqlTextResultChunkProvider();
			qso.execute(ssConnection, wg, results);
			assertEquals(results.getUpdateCount(), 0);

			// need to refresh the wg as we have changed the sg beneath it
			WorkerGroupFactory.purgeInstance(ssConnection, wg);
			wg = null;
			wg = WorkerGroupFactory.newInstance(ssConnection, sg, db);

			for (int i = 10; i < 20; ++i) {
				results = new MysqlTextResultChunkProvider();
				utilInsertTableTwoColumns(i, foo, wg, db, results);
				assertEquals(1, results.getUpdateCount());
			}

			for (int i = 0; i < 20; ++i) {
				KeyValue dv = foo.getDistValue(ssConnection.getCatalogDAO());
				dv.get("id").setValue(i);
				qso = new QueryStepSelectByKeyOperation(db, dv,
						"select * from " + foo.getNameAsIdentifier() + " where id = " + i);
				results = new MysqlTextResultChunkProvider();
				qso.execute(ssConnection, wg, results);
				assertEquals(1, results.getNumRowsAffected());
			}

			WorkerGroupFactory.returnInstance(ssConnection, wg);
			wg = null;

			PersistentGroup expCntSG;
			if ( testLastGenOnly )
				expCntSG = new PersistentGroup(sg.getLastGen().getStorageSites());
			else
				expCntSG = new PersistentGroup(sg.getStorageSites());

			wg = WorkerGroupFactory.newInstance(ssConnection, expCntSG, db);
			qso = new QueryStepSelectAllOperation(db, dm,
					"select * from " + foo.getNameAsIdentifier());
			results = new MysqlTextResultChunkProvider();
			qso.execute(ssConnection, wg, results);
			assertEquals(expectedCount, results.getNumRowsAffected());
		} finally {
			WorkerGroupFactory.returnInstance(ssConnection, wg);
		}
	}
	
	@Test
	public void addGenRandomTableNetNew() throws Throwable {
		final PersistentSite ss1 = catalogDAO.findPersistentSite(SITE1_NAME);
		final PersistentSite ss2 = catalogDAO.findPersistentSite(SITE2_NAME);
		final PersistentSite ss3 = catalogDAO.findPersistentSite(SITE3_NAME);
		DistributionModel dm = ssConnection.getCatalogDAO().findDistributionModel(RandomDistributionModel.MODEL_NAME);
		addGenToTable(dm, new PersistentSite[] { ss1, ss2}, new PersistentSite[] { ss3 }, 10, true);
	}

	@Test
	public void addGenRandomTableGrowGroup() throws Throwable {
		final PersistentSite ss1 = catalogDAO.findPersistentSite(SITE1_NAME);
		final PersistentSite ss2 = catalogDAO.findPersistentSite(SITE2_NAME);
		final PersistentSite ss3 = catalogDAO.findPersistentSite(SITE3_NAME);
		DistributionModel dm = ssConnection.getCatalogDAO().findDistributionModel(RandomDistributionModel.MODEL_NAME);
		addGenToTable(dm, new PersistentSite[] { ss1, ss2}, new PersistentSite[] { ss1, ss2, ss3 }, 20, true);
	}

	@Test
	public void addGenRandomTableNotNetNew() throws Throwable {
		final PersistentSite ss1 = catalogDAO.findPersistentSite(SITE1_NAME);
		final PersistentSite ss2 = catalogDAO.findPersistentSite(SITE2_NAME);
		final PersistentSite ss3 = catalogDAO.findPersistentSite(SITE3_NAME);
		DistributionModel dm = ssConnection.getCatalogDAO().findDistributionModel(RandomDistributionModel.MODEL_NAME);
		addGenToTable(dm, new PersistentSite[] { ss1, ss2, ss3}, new PersistentSite[] { ss1, ss2 }, 20, false);
	}

	@Test
	public void addGenBroadcastTableNetNew() throws Throwable {
		final PersistentSite ss1 = catalogDAO.findPersistentSite(SITE1_NAME);
		final PersistentSite ss2 = catalogDAO.findPersistentSite(SITE2_NAME);
		final PersistentSite ss3 = catalogDAO.findPersistentSite(SITE3_NAME);
		DistributionModel dm = ssConnection.getCatalogDAO().findDistributionModel(BroadcastDistributionModel.MODEL_NAME);
		addGenToTable(dm, new PersistentSite[] { ss1, ss2}, new PersistentSite[] { ss3 }, 20, true);
	}

	@Test
	public void addGenBroadcastTableGrowGroup() throws Throwable {
		final PersistentSite ss1 = catalogDAO.findPersistentSite(SITE1_NAME);
		final PersistentSite ss2 = catalogDAO.findPersistentSite(SITE2_NAME);
		final PersistentSite ss3 = catalogDAO.findPersistentSite(SITE3_NAME);
		DistributionModel dm = ssConnection.getCatalogDAO().findDistributionModel(BroadcastDistributionModel.MODEL_NAME);
		addGenToTable(dm, new PersistentSite[] { ss1, ss2}, new PersistentSite[] { ss1, ss2, ss3 }, 20, true);
	}

	@Test
	public void addGenBroadcastTableNoNetNew() throws Throwable {
		final PersistentSite ss1 = catalogDAO.findPersistentSite(SITE1_NAME);
		final PersistentSite ss2 = catalogDAO.findPersistentSite(SITE2_NAME);
		final PersistentSite ss3 = catalogDAO.findPersistentSite(SITE3_NAME);
		DistributionModel dm = ssConnection.getCatalogDAO().findDistributionModel(BroadcastDistributionModel.MODEL_NAME);
		addGenToTable(dm, new PersistentSite[] { ss1, ss2, ss3}, new PersistentSite[] { ss1, ss2 }, 20, false);
	}

	public void createRangeTwoTables(String sgName, String tableName1, String tableName2, String rangeName, PersistentSite ss1, PersistentSite ss2) throws Throwable {
		UserDatabase db = ssConnection.getPersistentDatabase();
		DistributionModel dm = catalogDAO.findDistributionModel(RangeDistributionModel.MODEL_NAME);
		PersistentGroup sg = utilCreateStorageGroup(sgName, Arrays.asList(new PersistentSite[] { ss1, ss2 }));
		DistributionRange dr = utilCreateDistRange(rangeName, sg, "int");
		
		UserTable foo = utilCreateTableTwoColumns(tableName1, sg, dm, db);
		QueryStepDDLOperation createTable1DDL =
				new QueryStepDDLOperation(db, new SQLCommand(foo.getCreateTableStmt()), null);
		createTable1DDL.addCatalogUpdate(foo);
		createTable1DDL.addCatalogUpdate(new RangeTableRelationship(foo, dr));

		UserTable foobar = utilCreateTableTwoColumns(tableName2, sg, dm, db);
		QueryStepDDLOperation createTable2DDL =
				new QueryStepDDLOperation(db, new SQLCommand(foobar.getCreateTableStmt()), null);
		createTable2DDL.addCatalogUpdate(foobar);
		createTable2DDL.addCatalogUpdate(new RangeTableRelationship(foobar, dr));

		WorkerGroup wg = WorkerGroupFactory.newInstance(ssConnection, sg, db);
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();
		createTable1DDL.execute(ssConnection, wg, results);
		assertEquals(0, results.getUpdateCount());
		results = new MysqlTextResultChunkProvider();
		createTable2DDL.execute(ssConnection, wg, results);
		assertEquals(0, results.getUpdateCount());

		for (int i = 0; i < 10; ++i) {
			results = new MysqlTextResultChunkProvider();
			utilInsertTableTwoColumns(i, foo, wg, db, results);
			assertEquals(1, results.getUpdateCount());
			results = new MysqlTextResultChunkProvider();
			utilInsertTableTwoColumns(i, foobar, wg, db, results);
			assertEquals(1, results.getUpdateCount());
		}
		
		for (int i = 0; i < 10; ++i) {
			KeyValue dv = foo.getDistValue(ssConnection.getCatalogDAO());
			dv.get("id").setValue(i);
			QueryStepOperation qso = new QueryStepSelectByKeyOperation(db, dv, 
					"select * from " + foo.getNameAsIdentifier() + " foo, " + foobar.getNameAsIdentifier() + " foobar where foo.id=foobar.id and foo.id = " + i);
			results = new MysqlTextResultChunkProvider();
			qso.execute(ssConnection, wg, results);
			assertEquals(1, results.getNumRowsAffected());
		}

		WorkerGroupFactory.returnInstance(ssConnection, wg);
	}
	
	@Test
	public void addRangeTwoTables() throws Throwable {
		final String sgName = nameForTest(SG_NAME);
		final String tableName1 = nameForTest(TABLE_NAME);
		final String tableName2 = nameForTest(TABLE2_NAME);
		final String rangeName = nameForTest(RANGE_NAME);
		final PersistentSite ss1 = catalogDAO.findPersistentSite(SITE1_NAME);
		final PersistentSite ss2 = catalogDAO.findPersistentSite(SITE2_NAME);
		createRangeTwoTables(sgName, tableName1, tableName2, rangeName, ss1, ss2);
	}
	
	@Test
	public void addRangeGeneration() throws Throwable {
		MysqlTextResultChunkProvider results;

		final String sgName = nameForTest(SG_NAME);
		final String tableName1 = nameForTest(TABLE_NAME);
		final String tableName2 = nameForTest(TABLE2_NAME);
		final String rangeName = nameForTest(RANGE_NAME);
		final PersistentSite ss1 = catalogDAO.findPersistentSite(SITE1_NAME);
		final PersistentSite ss2 = catalogDAO.findPersistentSite(SITE2_NAME);
		final PersistentSite ss3 = catalogDAO.findPersistentSite(SITE3_NAME);
		createRangeTwoTables(sgName, tableName1, tableName2, rangeName, ss1, ss2);
		
		UserDatabase db = ssConnection.getPersistentDatabase();
		UserTable foo = db.getTableByName(tableName1);
		DistributionModel dm = catalogDAO.findDistributionModel(RangeDistributionModel.MODEL_NAME);
		
		PersistentGroup sg = ssConnection.getCatalogDAO().findPersistentGroup(sgName);
		WorkerGroup wg = WorkerGroupFactory.newInstance(ssConnection, sg, db);
		try {
			QueryStepOperation qso = new QueryStepAddGenerationOperation(sg, Arrays.asList(new PersistentSite[] { ss3 }),CacheInvalidationRecord.GLOBAL);
			results = new MysqlTextResultChunkProvider();
			qso.execute(ssConnection, wg, results);
			assertEquals(results.getUpdateCount(), 0);
		} finally {
			// need to refresh the wg as we have changed the sg beneath it
			WorkerGroupFactory.purgeInstance(ssConnection, wg);
		}
		wg = WorkerGroupFactory.newInstance(ssConnection, sg, db);
		try {
			for (int i = 10; i < 20; ++i) {
				results = new MysqlTextResultChunkProvider();
				utilInsertTableTwoColumns(i, foo, wg, db, results);
				assertEquals(1, results.getUpdateCount());
			}

			for (int i = 0; i < 20; ++i) {
				KeyValue dv = foo.getDistValue(ssConnection.getCatalogDAO());
				dv.get("id").setValue(i);
				QueryStepOperation qso = new QueryStepSelectByKeyOperation(db, dv, 
						"select * from " + foo.getNameAsIdentifier() + " where id = " + i);
				results = new MysqlTextResultChunkProvider();
				qso.execute(ssConnection, wg, results);
				assertEquals("On record " + i, 1, results.getNumRowsAffected());
			}
		} finally {
			WorkerGroupFactory.returnInstance(ssConnection, wg);
		}
		
		PersistentGroup lastGenSG = new PersistentGroup(sg.getLastGen().getStorageSites());
		wg = WorkerGroupFactory.newInstance(ssConnection, lastGenSG, db);
		try {
			QueryStepOperation qso = new QueryStepSelectAllOperation(db, dm, 
					"select * from " + foo.getNameAsIdentifier());
			results = new MysqlTextResultChunkProvider();
			qso.execute(ssConnection, wg, results);
			assertEquals(10, results.getNumRowsAffected());
		} finally {
			WorkerGroupFactory.returnInstance(ssConnection, wg);
		}
	}
	
	private void utilInsertTableTwoColumns(int id, UserTable ut, WorkerGroup wg, UserDatabase db, DBResultConsumer results) throws Throwable {
		KeyValue dv = ut.getDistValue(ssConnection.getCatalogDAO());
		dv.get("id").setValue(id);
		QueryStepOperation qso = 
				new QueryStepInsertByKeyOperation(db, dv,
						"insert into " + ut.getNameAsIdentifier() + " values (" + id + ", 'value" + id + "')");
		qso.execute(ssConnection, wg, results);
	}
	
	private UserTable utilCreateTableTwoColumns(String name, PersistentGroup sg, DistributionModel dm, UserDatabase db) {
		UserTable ut = new UserTable(name, sg, dm, db, TableState.SHARED, PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
		UserColumn idCol = new UserColumn(ut, "id", Types.INTEGER, "int");
		idCol.setHashPosition(1);
		new UserColumn(ut, "val", Types.VARCHAR, "varchar", 20);
		ut.setCreateTableStmt("CREATE TABLE " + ut.getNameAsIdentifier() + " (id int, val varchar(20))");

		return ut;
	}
}
