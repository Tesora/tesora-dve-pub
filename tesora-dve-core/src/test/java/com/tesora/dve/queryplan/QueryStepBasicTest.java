package com.tesora.dve.queryplan;

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
// NOPMD by doug on 04/12/12 12:05 PM

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.*;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAOAccessor;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.test.simplequery.SimpleQueryTest;
import com.tesora.dve.worker.AggregationGroup;
import com.tesora.dve.worker.DynamicGroup;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.UserCredentials;

public class QueryStepBasicTest extends PETest {

	static {
		logger = Logger.getLogger(QueryStepBasicTest.class);
	}
	
	UserDatabase db;
	PersistentGroup sg;
	DynamicPolicy dynamicPolicy;

	SSConnectionProxy conProxy;
	SSConnection ssConnection;
	UserTable foo;
	
	QueryPlan plan;
	
	int currentId = 10;

	@BeforeClass
	public static void setup() throws Throwable {
		Class<?> bootClass = PETest.class;
		TestCatalogHelper.createTestCatalog(bootClass,2);
		bootHost = BootstrapHost.startServices(bootClass);
        populateMetadata(SimpleQueryTest.class, Singletons.require(HostService.class).getProperties());
	}
	
	@Before
	public void setupTest() throws PEException, SQLException {
		conProxy = new SSConnectionProxy();
		ssConnection = SSConnectionAccessor.getSSConnection(conProxy);
		SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
		ssConnection.startConnection(new UserCredentials(bootHost.getProperties()));
		ssConnection.setPersistentDatabase(catalogDAO.findDatabase("TestDB"));
		db = ssConnection.getPersistentDatabase();
		sg = db.getDefaultStorageGroup();
		sg.getStorageSites();
		dynamicPolicy = ssConnection.getCatalogDAO().findDefaultProject().getDefaultPolicy();
        populateSites(SimpleQueryTest.class, Singletons.require(HostService.class).getProperties());
		++currentId;
		plan = new QueryPlan();
		foo = db.getTableByName("foo");
	}
	
	@After
	public void cleanupTest() throws PEException {
		if(plan != null)
			plan.close();
		if(conProxy != null)
			conProxy.close();
		if(ssConnection != null)
			ssConnection.close();
	}
	
	@Test(expected=PEException.class)
	public void emptyExecuteStep() throws Throwable {
		QueryStep step1 = new QueryStep(sg, null);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		plan.executeStep(ssConnection, new MysqlTextResultCollector());
		ssConnection.userRollbackTransaction();
	}
	
	@Test
	public void emptyResultSet() throws Throwable {
		QueryStepOperation step1op1 = new QueryStepSelectAllOperation(db, foo.getDistributionModel(),
				"select * from foo where 0=1");
		QueryStep step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertTrue(results.hasResults());
		assertEquals(0, results.getNumRowsAffected());
	}
	
	@Test
	public void insertOneRecord() throws Throwable {
		UserTable t = db.getTableByName("foo"); 
		KeyValue distValue = t.getDistValue(ssConnection.getCatalogDAO());
		distValue.get("id").setValue(new Integer(currentId));

		QueryStepOperation step1op1 = new QueryStepInsertByKeyOperation(db, distValue, "insert into foo values ("+currentId+", 'Hello')");
		QueryStep step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
		assertEquals(1, results.getUpdateCount());
	}
	
	@Test
	public void setupStep() throws Throwable {
		UserTable t = db.getTableByName("foo"); 
		KeyValue distValue = t.getDistValue(ssConnection.getCatalogDAO());
		distValue.get("id").setValue(new Integer(currentId));
		
		QueryStepOperation step1op1 = new QueryStepInsertByKeyOperation(db, distValue,
				"insert into foo values ("+currentId+", 'setup')");
		QueryStepOperation step1op2 = new QueryStepSelectAllOperation(db, foo.getDistributionModel(),
				"select * from foo where id = "+currentId);
		QueryStep step1 = new QueryStep(sg, step1op2).addSetupOperation(step1op1);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertTrue(results.hasResults());
		assertEquals(1, results.getNumRowsAffected());
		plan.close();
	}
	
	@Test
	public void redistEmptyResult() throws Throwable {
		UserTable bar = db.getTableByName("bar"); 
		String tempName = UserTable.getNewTempTableName();
		StorageGroup tempSG = new DynamicGroup(dynamicPolicy, StorageGroup.GroupScale.SMALL);
		
		QueryStepOperation step1op1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand("select * from bar where 0=1"), bar.getDistributionModel())
			.toTempTable(tempSG, db, tempName);
		QueryStepOperation step2op1 = new QueryStepSelectAllOperation(db, bar.getDistributionModel(),
				"select * from "+tempName+" where id > 1999");
		QueryStep step1 = new QueryStep(sg, step1op1);
		QueryStep step2 = new QueryStep(tempSG, step2op1).addDependencyStep(step1);
		plan.addStep(step2);
				
		MysqlTextResultCollector rc = new MysqlTextResultCollector(true);
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		assertEquals(0, rc.getRowData().size());
		plan.close();
	}

	@Test
	public void redistToRandomTemp() throws Throwable {
		UserTable bar = db.getTableByName("bar"); 
		String tempName = UserTable.getNewTempTableName();
		StorageGroup tempSG = new AggregationGroup(dynamicPolicy);
		
		QueryStepOperation step1op1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand("select id c1, value c2 from bar"), bar.getDistributionModel())
			.toTempTable(tempSG, db, tempName);
		QueryStepOperation step2op1 = new QueryStepSelectAllOperation(db,
				StaticDistributionModel.SINGLETON,
				"select c1, c2 from "+tempName+" where c1 > 1999");
		QueryStep step1 = new QueryStep(sg, step1op1);
		QueryStep step2 = new QueryStep(tempSG, step2op1).addDependencyStep(step1);
		plan.addStep(step2);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertTrue(results.hasResults());
		assertEquals(2, results.getNumRowsAffected());
	}

	@Test
	public void redistComputed() throws Throwable {
		// this tests redistributing on a computed column, and also shows that 
		// you can redistribute any result set (such as a join between colocated tables)
		//
		UserTable bar = db.getTableByName("bar");
		String temp1Name = UserTable.getNewTempTableName();
		String temp2Name = UserTable.getNewTempTableName();
		
		QueryStepOperation step1op1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand("select concat('a',id), concat(value,'a') from bar where id > 1999"), bar.getDistributionModel())
			.toTempTable(sg, db, temp1Name)
			.distributeOn(Arrays.asList(new String[]{"concat('a',id)"}));
		QueryStepOperation step2op1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand("select `concat('a',id)` c1, `concat(value,'a')` c2 from " + temp1Name), StaticDistributionModel.SINGLETON)
			.toTempTable(sg, db, temp2Name);
		QueryStepOperation step3op1 = new QueryStepSelectAllOperation(db,
				BroadcastDistributionModel.SINGLETON,
				"select c1, c2 from "+temp2Name);
		QueryStep step1 = new QueryStep(sg, step1op1);
		QueryStep step2 = new QueryStep(sg, step2op1); //.addDependencyStep(step1);
		QueryStep step3 = new QueryStep(sg, step3op1); //.addDependencyStep(step2);
		step3.addDependencyStep(step1);
		step3.addDependencyStep(step2);
		plan.addStep(step3);
				
		MysqlTextResultCollector rc = new MysqlTextResultCollector(true);
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		assertEquals(2, rc.getRowData().size());
		plan.close();
	}

	@Test
	public void redistToUserAndJoin() throws Throwable {
		UserTable bar = db.getTableByName("bar"); 
		String tempName = UserTable.getNewTempTableName();
		
		QueryStepOperation step1op1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand("select id c1, value c2 from bar"), bar.getDistributionModel())
			.toTempTable(sg, db, tempName);
		QueryStepOperation step2op1 = new QueryStepSelectAllOperation(db, foo.getDistributionModel(),
				"select * from "+tempName+" t, foo f where t.c1 = f.id");
		QueryStep step1 = new QueryStep(sg, step1op1);
		QueryStep step2 = new QueryStep(sg, step2op1).addDependencyStep(step1);
		plan.addStep(step2);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector(true);
		plan.executeStep(ssConnection, results);
		assertTrue(results.hasResults());
		results.printRows();
		assertEquals(2, results.getNumRowsAffected());
	}

	@Test
	public void createTable() throws Throwable {
		DistributionModel dm = catalogDAO.findDistributionModel(BroadcastDistributionModel.MODEL_NAME);
		UserTable ut = catalogDAO.createUserTable(db, "foobar", dm, sg, PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
		assertEquals(0, ut.getUserColumns().size());

		UserColumn c1 = catalogDAO.createUserColumn(ut, "col1", Types.INTEGER, "INT", 10);
		assertEquals(1, ut.getUserColumns().size());
		c1.setHashPosition(1);
		ut.addKey(new Key("PRIMARY",IndexType.BTREE,ut,Collections.singletonList(new KeyColumn(c1,null,1,-1L)),1));
		UserColumn c2 = catalogDAO.createUserColumn(ut, "col2", Types.VARCHAR, "VARCHAR", 10);
		UserColumn c3 = catalogDAO.createUserColumn(ut, "col3", Types.INTEGER, "INT", 10);
		assertEquals(3, ut.getUserColumns().size());

		// Create table
		QueryStepDDLOperation step1op1 = new QueryStepDDLOperation(db, new SQLCommand("create table foobar (col1 int, col2 varchar(10), col3 int)"),null);
		step1op1.addCatalogUpdate(ut);
		step1op1.addCatalogUpdate(c1);
		step1op1.addCatalogUpdate(c2);
		step1op1.addCatalogUpdate(c3);
		QueryStep step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
		assertEquals(0, results.getUpdateCount());
		plan.close();
		assertEquals(3, ut.getUserColumns().size());
		
		// insert a record
		QueryPlan insertPlan = new QueryPlan();
		KeyValue distValue = ut.getDistValue(ssConnection.getCatalogDAO());
		distValue.get("col1").setValue(new Integer(currentId));
		QueryStepOperation step2op1 = new QueryStepInsertByKeyOperation(db, distValue, "insert into foobar values ("+currentId+", 'Hello', 1)");
		QueryStep step2 = new QueryStep(sg, step2op1);
		insertPlan.addStep(step2);
		logger.debug("Executing QueryPlan:\n"+insertPlan.asXML());
		results = new MysqlTextResultCollector();
		insertPlan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
		assertEquals(1, results.getUpdateCount());
		insertPlan.close();
		
		// retrieve the record
		QueryPlan resultPlan = new QueryPlan();
		QueryStepOperation step3op1 = new QueryStepSelectAllOperation(db, ut.getDistributionModel(),
				"select * from foobar");
		QueryStep step3 = new QueryStep(sg, step3op1);
		resultPlan.addStep(step3);
		logger.debug("Executing QueryPlan:\n"+resultPlan.asXML());
		results = new MysqlTextResultCollector();
		resultPlan.executeStep(ssConnection, results);
		assertTrue(results.hasResults());
		assertEquals(1, results.getNumRowsAffected());
		resultPlan.close();
		
		// verify the catalog
		EntityManager em = CatalogDAOAccessor.getEntityManager(catalogDAO);
		em.clear();
		Query q = em.createQuery("from UserTable ut where ut.name = 'foobar'");
		@SuppressWarnings("unchecked")
		List<UserTable> tableList = q.getResultList();
		assertEquals(1, tableList.size());
		UserTable newTable = tableList.get(0);
		assertEquals(3, newTable.getUserColumns().size());
		assertTrue(c1.equals(newTable.getUserColumn(c1.getName())));
		assertTrue(c2.equals(newTable.getUserColumn(c2.getName())));
		assertTrue(c3.equals(newTable.getUserColumn(c3.getName())));
	}
	
	@Test
	public void createTableTxn() throws Throwable {
		DistributionModel dm = catalogDAO.findDistributionModel(BroadcastDistributionModel.MODEL_NAME);
		UserTable ut = new UserTable("foobear", sg, dm, db, TableState.SHARED, PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
		UserColumn c1 = new UserColumn(ut, "col1", Types.INTEGER, "INT");
		UserColumn c2 = new UserColumn(ut, "col2", Types.VARCHAR, "VARCHAR", 10);
		UserColumn c3 = new UserColumn(ut, "col3", Types.INTEGER, "INT");
		
		ssConnection.userBeginTransaction();
		try {
			QueryStepDDLOperation step1op1 = new QueryStepDDLOperation(db, new SQLCommand("create table foobear (col1 int, col2 varchar(10), col3 int)"), null);
			step1op1.addCatalogUpdate(ut);
			step1op1.addCatalogUpdate(c1);

			step1op1.addCatalogUpdate(c2);
			step1op1.addCatalogUpdate(c3);
			QueryStep step1 = new QueryStep(sg, step1op1);
			plan.addStep(step1);
			logger.debug("Executing QueryPlan:\n"+plan.asXML());
			MysqlTextResultCollector results = new MysqlTextResultCollector();
			plan.executeStep(ssConnection, results);
			assertFalse(results.hasResults());
			assertEquals(0, results.getUpdateCount());
			ssConnection.userCommitTransaction();
		} catch (Exception e) {
			ssConnection.userRollbackTransaction();
			throw e;
		}
	}
	
	@Test
	public void updateRow() throws Throwable {
		UserTable t = db.getTableByName("foo"); 
		
		QueryStepOperation step1op1 = new QueryStepUpdateAllOperation(db, t.getDistributionModel(), "update foo set value = 'bob' where id = 1");
		QueryStep step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
		assertEquals(1, results.getUpdateCount());
	}
	
	@Test
	public void updateMultiRow() throws Throwable {
		UserTable t = db.getTableByName("foo"); 
		
		QueryStepOperation step1op1 = new QueryStepUpdateAllOperation(db, t.getDistributionModel(), "update foo set value = 'bob' where id < 3");
		QueryStep step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
		assertEquals(2, results.getUpdateCount());
	}
	
	@Test
	public void createAndDropDatabase() throws Throwable {
		String databaseName = "MyTestDB";
		UserDatabase newDB = new UserDatabase(databaseName, sg);
		QueryStepOperation step1op1 = new QueryStepCreateDatabaseOperation(newDB, null);
		QueryStep step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
		assertEquals(2, results.getUpdateCount());
		plan.close();

		// create a table as well to test if drop database removes the table 
		// and column information from the metadata
		DistributionModel dm = catalogDAO.findDistributionModel(BroadcastDistributionModel.MODEL_NAME);
		UserTable ut = new UserTable("foobar", sg, dm, newDB, TableState.SHARED, PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);
		UserColumn c1 = catalogDAO.createUserColumn(ut, "col1", Types.INTEGER, "INT", 10);
		assertEquals(1, ut.getUserColumns().size());
		c1.setHashPosition(1);
		ut.addKey(new Key("PRIMARY",IndexType.BTREE,ut,Collections.singletonList(new KeyColumn(c1,null,1, -1L)),1));
		UserColumn c2 = catalogDAO.createUserColumn(ut, "col2", Types.VARCHAR, "VARCHAR", 10);
		UserColumn c3 = catalogDAO.createUserColumn(ut, "col3", Types.INTEGER, "INT", 10);
		newDB.addUserTable(ut);
		QueryStepDDLOperation step1op2 = new QueryStepDDLOperation(newDB, new SQLCommand("create table foobar (col1 int, col2 varchar(10), col3 int)"), null);
		step1op2.addCatalogUpdate(ut);
		step1op2.addCatalogUpdate(c1);
		step1op2.addCatalogUpdate(c2);
		step1op2.addCatalogUpdate(c3);
		step1 = new QueryStep(sg, step1op2);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
		assertEquals(0, results.getUpdateCount());
		plan.close();
		
		// verify the table and columns are created in the test database
		assertTrue("Catalog must contain table " + ut.displayName(),
				catalogDAO.findAllUserTables().contains(ut));
		assertEquals(3, ut.getUserColumns().size());
		List<UserColumn> userColumns = catalogDAO.findAllUserColumns();
		assertTrue("Catalog must contain column " + c1.getName(),
				userColumns.contains(c1));
		assertTrue("Catalog must contain column " + c2.getName(),
				userColumns.contains(c2));
		assertTrue("Catalog must contain column " + c3.getName(),
				userColumns.contains(c3));
		
		plan = new QueryPlan();
		step1op1 = new QueryStepDropDatabaseOperation(newDB,null);
		step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		logger.debug("Executing QueryPlan:\n"+plan.asXML());
		results = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
		
		// make sure the database is deleted from the user_database as well
		try {
			assertNull("Database '" + databaseName
					+ "' should have been deleted from user_database",
					catalogDAO.findDatabase(databaseName));
		} catch (PEException e) {
			// make sure a specific exception is thrown
			assertEquals("Expected exactly one UserDatabase for name "
					+ databaseName, e.getMessage());
		}
		
		// make sure the table information is deleted from user_table and user_column
		userColumns = catalogDAO.findAllUserColumns();
		assertFalse("Catalog must not contain table " + ut.displayName(),
				catalogDAO.findAllUserTables().contains(ut));
		assertFalse("Catalog must not contain column " + c1.getName(),
				userColumns.contains(c1));
		assertFalse("Catalog must not contain column " + c2.getName(),
				userColumns.contains(c2));
		assertFalse("Catalog must not contain column " + c3.getName(),
				userColumns.contains(c3));
	}

	@Test
	public void statementFailure() throws Throwable {
		// The point of this test is to make sure we can continue to execute statements
		// after a statement fails. i.e. make sure the "Cannot re-allocate active StorageGroup"
		// problem is fixed.
		UserDatabase newDB = new UserDatabase("TestDB", sg);
		QueryStepOperation step1op1 = new QueryStepCreateDatabaseOperation(newDB, null);
		QueryStep step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		try {
			plan.executeStep(ssConnection, new MysqlTextResultCollector());
			fail("Exception expected on creation of duplicate database TestDB");	}
		catch (PEException re) {
			// this is expected...
		}
		
		plan = new QueryPlan();
		newDB = new UserDatabase("TestDB2", sg);
		step1op1 = new QueryStepCreateDatabaseOperation(newDB, null);
		step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		MysqlTextResultCollector results = new MysqlTextResultCollector();
		// this shouldn't generate an exception...
		plan.executeStep(ssConnection, results);
		assertFalse(results.hasResults());
	}

	@Test
	public void txnBasic() {
		// Adding this empty test exposed a bug with the test above
		// namely that having the catalog on MyISAM is bad
		// we'll leave this here for now in case of regression
	}
}
