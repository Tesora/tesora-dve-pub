package com.tesora.dve.test.simplequery;

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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.server.connectionmanager.*;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepInsertByKeyOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepUpdateByKeyOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.UserCredentials;

// this test will thrash the build machine (it's for performance testing with YourKit)
@Test(enabled= false, groups = { "NonSmokeTest" })
public class SimpleConcurrencyTestNG extends PETest /* do not change this to a SchemaTest */ {

	static {
		logger = Logger.getLogger(SimpleConcurrencyTestNG.class);
	}

	static final String COUNT_ROWS_SELECT = "select * from foo";

	static final int READ_COUNT = 100000;
	static final int READ_THREADS = 12;
	static final int WRITE_COUNT = 50000;
	static final int WRITE_THREADS = 8;

	@BeforeClass
	public static void setup() throws Throwable {
		TestCatalogHelper.createTestCatalog(PETest.class,2);
		bootHost = BootstrapHost.startServices(PETest.class);
        populateMetadata(SimpleQueryTest.class, Singletons.require(HostService.class).getProperties());
        populateSites(SimpleQueryTest.class, Singletons.require(HostService.class).getProperties());
	}

	@Test(enabled=false)
	public void readConcurrencyTest() throws InterruptedException, ExecutionException {
		long startTime = System.currentTimeMillis();
		ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>();
		ExecutorService execService = Executors.newCachedThreadPool();
		for (int i = 1; i < READ_THREADS; ++i) {
			final int threadId = i;
			futures.add(execService.submit(new Callable<Void>() {
				public Void call() throws Exception {
					try {
						doStartSelectThread(threadId);
					} catch (Throwable t) {
						throw new Exception(t);
					}
					return null;
				}
			}));
		}
		for(Future<Void> f : futures)
			f.get();
		long executeTime = System.currentTimeMillis() - startTime;
		System.out.println("Read execution took " + executeTime / 1000 + " seconds");
	}

	private void doStartSelectThread(int threadId) throws Throwable {
		
//		System.out.println("Starting thread " + threadId);
		
		CatalogDAO catalogDAO = CatalogDAOFactory.newInstance();
		SSConnectionProxy conProxy = new SSConnectionProxy();
		SSConnection ssConnection = SSConnectionAccessor.getSSConnection(conProxy);
		SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
		ssConnection.startConnection(new UserCredentials(PEConstants.ROOT, PEConstants.PASSWORD));
		ssConnection.setPersistentDatabase(catalogDAO.findDatabase("TestDB"));
		UserDatabase db = ssConnection.getPersistentDatabase();
		PersistentGroup sg = db.getDefaultStorageGroup();
		sg.getStorageSites();
		UserTable foo = db.getTableByName("foo");

		for (int i = 0; i < READ_COUNT; ++i) {
			int currentId = READ_COUNT % 5 + 1;
			KeyValue distValue = foo.getDistValue(catalogDAO);
			distValue.get("id").setValue(new Integer(currentId));

			QueryPlan plan = new QueryPlan();
			QueryStepOperation step1op1 = new QueryStepInsertByKeyOperation(db, distValue, "select * from foo where id = "+currentId);
			QueryStep step1 = new QueryStep(sg, step1op1);
			plan.addStep(step1);
			MysqlTextResultCollector results = new MysqlTextResultCollector();
			plan.executeStep(ssConnection, results);
			assertTrue(results.hasResults());
//			assertEquals(1, results.getNumRowsAffected());
			plan.close();
		}
		conProxy.close();
		
//		System.out.println("Thread " + threadId + " ran " + READ_COUNT + " selects");

	}
	
	@Test(enabled=false)
	public void updateConcurrencyTest() throws InterruptedException, ExecutionException {
		ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>();
		ExecutorService execService = Executors.newCachedThreadPool();
		for (int i = 1; i < WRITE_THREADS; ++i) {
			final int threadId = i;
			futures.add(execService.submit(new Callable<Void>() {
				public Void call() throws Exception {
					try {
						doStartUpdateThread(threadId);
					} catch (Throwable t) {
						throw new Exception(t);
					}
					return null;
				}
			}));
		}
		for(Future<Void> f : futures)
			f.get();
	}

	private void doStartUpdateThread(int threadId) throws Throwable {
		
//		System.out.println("Starting thread " + threadId);
		
		CatalogDAO catalogDAO = CatalogDAOFactory.newInstance();
		SSConnectionProxy conProxy = new SSConnectionProxy();
		SSConnection ssConnection = SSConnectionAccessor.getSSConnection(conProxy);
		SSConnectionAccessor.setCatalogDAO(ssConnection, catalogDAO);
		ssConnection.startConnection(new UserCredentials(PEConstants.ROOT, PEConstants.PASSWORD));
		ssConnection.setPersistentDatabase(catalogDAO.findDatabase("TestDB"));
		UserDatabase db = ssConnection.getPersistentDatabase();
		PersistentGroup sg = db.getDefaultStorageGroup();
		sg.getStorageSites();
		UserTable foo = db.getTableByName("foo");

		for (int i = 0; i < WRITE_COUNT; ++i) {
			int currentId = WRITE_COUNT % 5 + 1;
			KeyValue distValue = foo.getDistValue(catalogDAO);
			distValue.get("id").setValue(new Integer(currentId));

			QueryPlan plan = new QueryPlan();
			QueryStepOperation step1op1 = new QueryStepUpdateByKeyOperation(db, distValue, new SQLCommand("update foo set value = 'value"+currentId+"' where id = "+currentId));
//			QueryStepOperation step1op1 = new QueryStepUpdateByKeyOperation(db, distValue, new SQLCommand("select * from foo where id = "+currentId));
			QueryStep step1 = new QueryStep(sg, step1op1);
			plan.addStep(step1);
			MysqlTextResultCollector results = new MysqlTextResultCollector();
			plan.executeStep(ssConnection, results);
			assertFalse(results.hasResults());
//			assertTrue(results.hasResults());
//			assertEquals(1, results.getNumRowsAffected());
			plan.close();
		}
		Thread.sleep(100);
		conProxy.close();
		
//		System.out.println("Thread " + threadId + " ran " + WRITE_COUNT + " selects");

	}
}
