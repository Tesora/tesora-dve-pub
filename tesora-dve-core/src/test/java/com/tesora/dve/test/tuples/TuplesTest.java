package com.tesora.dve.test.tuples;

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

import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.KeyTemplate;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.queryplan.QueryStepUpdateAllOperation;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.SSConnectionAccessor;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.UserCredentials;

//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TuplesTest extends PETest {

	static Logger logger = Logger.getLogger(TuplesTest.class);
	
	UserDatabase db;
	PersistentGroup sg;

	SSConnection ssConnection;
	SSConnectionProxy ssConnProxy;
	
	QueryPlan plan;
	
	@BeforeClass
	public static void setup() throws Exception {
		Class<?> bootClass = PETest.class;
		TestCatalogHelper.createTestCatalog(bootClass,2);
		bootHost = BootstrapHost.startServices(bootClass);
        populateMetadata(TuplesTest.class, Singletons.require(HostService.class).getProperties());
	}

	@Before
	public void setupTest() throws PEException, SQLException {
        populateSites(TuplesTest.class, Singletons.require(HostService.class).getProperties());
		plan = new QueryPlan();

		ssConnProxy = new SSConnectionProxy();
		ssConnection = SSConnectionAccessor.getSSConnection(ssConnProxy);
		SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
		ssConnection.startConnection(new UserCredentials(bootHost.getProperties()));
		ssConnection.setPersistentDatabase(catalogDAO.findDatabase("TestDB"));
		db = ssConnection.getPersistentDatabase();
		sg = db.getDefaultStorageGroup();
	}
	
	@After
	public void cleanupTest() throws PEException {
		plan.close();
		ssConnProxy.close();
	}

	@Test
	public void Ajoin2TempBroadcast() throws Throwable {
		//
		//	select t1.a, t1.b, t3,c, t4.p 
		//	from t1, t2
		//	where t1.a = t2.a and t1.b = t2.b
		//
		UserTable t1 = db.getTableByName("t1"); 
		String temp1Name = UserTable.getNewTempTableName();
		UserTable t2 = db.getTableByName("t2"); 
		String temp2Name = UserTable.getNewTempTableName();
		
		QueryStepOperation step1aOp1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand(ssConnection, "select a,b,c from t1"), t1.getDistributionModel())
			.toTempTable(sg, db, temp1Name);
		QueryStepOperation step1bOp1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand(ssConnection, "select a,b,p from t2"), t2.getDistributionModel())
			.toTempTable(sg, db, temp2Name);
		QueryStepOperation step2op1 = new QueryStepSelectAllOperation(ssConnection, db, BroadcastDistributionModel.SINGLETON,
				"select t1.a, t1.b, t1.c, t2.p from "+temp1Name+" t1, " + temp2Name + " t2 "
				+ "where t1.a = t2.a and t1.b = t2.b");
		QueryStep step1a = new QueryStep(sg, step1aOp1);
		QueryStep step1b = new QueryStep(sg, step1bOp1);
		QueryStep step2 = new QueryStep(sg, step2op1);
		step2.addDependencyStep(step1a);          
		step2.addDependencyStep(step1b);
		plan.addStep(step2);
		
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		assertEquals(6, rc.getRowData().size());
		plan.close();
	}
	
//	@Test 
//	public void A1() throws Throwable {
//		Ajoin2TempBroadcast();
//	}
//	
//	@Test 
//	public void A2() throws Throwable {
//		Ajoin2TempBroadcast();
//	}
	
	@Test
	public void Bjoin2TempBroadcastWithTrans() throws Throwable {
		ssConnection.userBeginTransaction();
		try {
			Ajoin2TempBroadcast();
			ssConnection.userCommitTransaction();
		} catch (Exception e) {
			try {
			ssConnection.userRollbackTransaction();
			} catch (Exception ee) {}
			throw e;
		}
	}
	
	@Test
	public void Cjoin2TempBroadcastTempStatic() throws Throwable {
		//
		//	select t1.a, t1.b, t3,c, t4.p 
		//	from t1, t2
		//	where t1.a = t2.a and t1.b = t2.b
		//
		UserTable t1 = db.getTableByName("t1"); 
		String temp1Name = UserTable.getNewTempTableName();
		UserTable t2 = db.getTableByName("t2"); 
		String temp2Name = UserTable.getNewTempTableName();
		
		QueryStepOperation step1aOp1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand(ssConnection, "select a,b,c from t1"), t1.getDistributionModel())
			.toTempTable(sg, db, temp1Name)
			.distributeOn(t1.getDistKey().asColumnList());
		QueryStepOperation step1bOp1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand(ssConnection, "select a,b,p from t2"), t2.getDistributionModel())
			.toTempTable(sg, db, temp2Name)
			.distributeOn(t2.getDistKey().asColumnList());
		QueryStepOperation step2op1 = new QueryStepSelectAllOperation(ssConnection, db, StaticDistributionModel.SINGLETON,
//				"select * from t2");
				"select t1.a, t1.b, t1.c, t2.p from "+temp1Name+" t1, " + temp2Name + " t2 "
				+ "where t1.a = t2.a and t1.b = t2.b");
		QueryStep step1a = new QueryStep(sg, step1aOp1);
		QueryStep step1b = new QueryStep(sg, step1bOp1);
		QueryStep step2 = new QueryStep(sg, step2op1);
		step2.addDependencyStep(step1a);
		step2.addDependencyStep(step1b);
		plan.addStep(step2);
		
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		assertEquals(6, rc.getRowData().size());
		plan.close();
	}

	@Test
	public void DjoinTempBroadcastTempStaticWithTrans() throws Throwable {
		try {
			ssConnection.userBeginTransaction();
			Cjoin2TempBroadcastTempStatic();
			ssConnection.userCommitTransaction();
		} catch (Exception e) {
			ssConnection.userRollbackTransaction();
			throw e;
		}
	}
	
	@Test
	public void groupByRecordCount() throws Throwable {
		//
		//	select t1.a, count(*) from t1 group by t1.a		
		//
		UserTable t1 = db.getTableByName("t1");
		String tempName = UserTable.getNewTempTableName();
		PersistentGroup tempGroup = sg.anySite();
		KeyTemplate distKey = new KeyTemplate();
		distKey.add(t1.getUserColumn("a"));
		
		QueryStepOperation step1aOp1 = new QueryStepMultiTupleRedistOperation(db, 
				new SQLCommand(ssConnection, "select a, count(*) ct from t1 group by t1.a"), t1.getDistributionModel())
			.toTempTable(tempGroup, db, tempName);
		QueryStepOperation step2op1 = new QueryStepSelectAllOperation(ssConnection, db, StaticDistributionModel.SINGLETON,
				"select "+tempName+".a as a, sum(ct) as \"count(*)\" from "+tempName+" group by a");
		QueryStep step1a = new QueryStep(sg, step1aOp1);
		QueryStep step2 = new QueryStep(tempGroup, step2op1);
		step2.addDependencyStep(step1a);
		plan.addStep(step2);
		
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertTrue(rc.hasResults());
		assertEquals(3, rc.getRowData().size());
		plan.close();
	}
//	
//	public void testLimit(String sql, int limit, int expectedCount) throws Throwable {
//		//
//		//	select t1.a, count(*) from t1 group by t1.a		
//		//
//		UserTable t1 = db.getTableByName("t1");
//		
//		QueryStepOperation step1aOp1 = new QueryStepSelectAllOperation(db, 
//				t1.getDistributionModel(), new SQLCommand(sql))
//		.setResultsLimit(limit);
//		QueryStep step1a = new QueryStep(sg, step1aOp1);
//		plan.addStep(step1a);
//		if (logger.isDebugEnabled())
//		logger.debug("Executing QueryPlan:\n"+plan.asXML());
//		boolean hasResults = plan.executeStep(ssConnection);
//		assertTrue(hasResults);
//		ResultCollector rc = plan.getResultCollector();
//		rc.printRows();
//		assertEquals(expectedCount, rc.getRowCount());
//		plan.close();
//	}
//	
//	@Test
//	public void noLimit() throws Throwable {
//		testLimit("select * from t1", -1, 9);
//	}
//	
//	@Test
//	public void limit0() throws Throwable {
//		testLimit("select * from t1 limit 0", 0, 0);
//	}
//	
//	@Test
//	public void limit1() throws Throwable {
//		testLimit("select * from t1 limit 1", 1, 1);
//	}
//	
//	@Test
//	public void limit2() throws Throwable {
//		testLimit("select * from t1 limit 2", 2, 2);
//	}
//	
	@Test
	public void groupByRecordCountWithTrans() throws Throwable {
		ssConnection.userBeginTransaction();
		try {
			groupByRecordCount();
			ssConnection.userCommitTransaction();
		} catch (Exception e) {
			ssConnection.userRollbackTransaction();
			throw e;
		}
	}
	
	private void updateMultiRow(UserTable t, int expected, String qry) throws Throwable {
		QueryPlan plan = new QueryPlan();
		QueryStepOperation step1op1 = new QueryStepUpdateAllOperation(ssConnection, db, t.getDistributionModel(), qry);
		QueryStep step1 = new QueryStep(sg, step1op1);
		plan.addStep(step1);
		
		MysqlTextResultCollector rc = new MysqlTextResultCollector();
		plan.executeStep(ssConnection, rc);
		assertFalse(rc.hasResults());
		assertEquals(expected, rc.getUpdateCount());
		plan.close();
	}
	
	@Test
	public void updateRowCount() throws Throwable {
		UserTable t1 = db.getTableByName("t1");
		UserTable t2 = db.getTableByName("t2");
		
		updateMultiRow(t1, 3, "update t1 set c = 10 where a = 1");
		updateMultiRow(t1, 3, "update t1 set c = 11 where a = 2");
		updateMultiRow(t1, 3, "update t1 set c = 12 where a = 3");
		
		updateMultiRow(t2, 3, "update t2 set p = 10 where a = 1");
		updateMultiRow(t2, 0, "update t2 set p = 11 where a = 2");
		updateMultiRow(t2, 3, "update t2 set p = 12 where a = 3");
		updateMultiRow(t2, 3, "update t2 set p = 13 where a = 4");
	}
	
	@Test
	public void stress() throws Throwable {
		if (Boolean.getBoolean("TuplesTest.stress")) {
			long delayms = Long.parseLong(System.getProperty("stress.delay", "0"));
			Thread.sleep(delayms);
			for (int i = 0; i < 1000; ++i) {
				Ajoin2TempBroadcast();
				Cjoin2TempBroadcastTempStatic();
				groupByRecordCount();
			}
		}
	}
}
