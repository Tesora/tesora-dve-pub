package com.tesora.dve.test.autoincrement;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.common.catalog.AutoIncrementTracker;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.test.distribution.DistributionTest;

public class AutoIncrementTrackerTest extends PETest {

	static UserDatabase db;
	static UserTable foo, bar;

	@BeforeClass
	public static void setup() throws Exception {
		Class<?> bootClass = PETest.class;
		TestCatalogHelper.createTestCatalog(bootClass,2);
		bootHost = BootstrapHost.startServices(bootClass);

        populateMetadata(AutoIncrementTrackerTest.class, Singletons.require(HostService.class).getProperties());
        populateSites(DistributionTest.class, Singletons.require(HostService.class).getProperties());

		db = catalogDAO.findDatabase(UserDatabase.DEFAULT);
		foo = db.getTableByName("foo");
		bar = db.getTableByName("bar");

		catalogDAO.begin();
		foo.addAutoIncr();
		bar.addAutoIncr();
		catalogDAO.commit();
	}

	@Before
	public void beforeTest() throws Exception {
		AutoIncrementTracker autoIncrBar = bar.getAutoIncr();
		AutoIncrementTracker autoIncrFoo = foo.getAutoIncr();

		catalogDAO.begin();
		autoIncrBar.reset(catalogDAO);
		autoIncrFoo.reset(catalogDAO);
		catalogDAO.commit();

		assertEquals("Tracker " + bar.getAutoIncr().getId() + " not reset: ", 1, bar.getAutoIncr().readNextValue(catalogDAO));
		assertEquals("Tracker " + foo.getAutoIncr().getId() + " not reset: ", 1, foo.getAutoIncr().readNextValue(catalogDAO));
	}

	@Test
	public void testGetNextValue() throws PEException {
		catalogDAO.refresh(foo);

		// Test that there are no gaps when the next block is gotten
		for (int i = 1; i < 26; ++i) {
			assertEquals(i, foo.getNextIncrValue(catalogDAO));
		}
	}

	@Test
	public void testGetIdBlock() throws PEException {
		catalogDAO.refresh(bar);

		// test that block requests increment ids correctly
		assertEquals(1, bar.getNextIncrValue(catalogDAO));
		assertEquals(2, bar.getNextIncrBlock(catalogDAO, 2));
		assertEquals(4, bar.getNextIncrValue(catalogDAO));
		assertEquals(5, bar.getNextIncrBlock(catalogDAO, 2));
		assertEquals(7, bar.getNextIncrValue(catalogDAO));
		assertEquals(8, bar.getNextIncrBlock(catalogDAO, 10));
		assertEquals(18, bar.getNextIncrValue(catalogDAO));
		assertEquals(19, bar.getNextIncrBlock(catalogDAO, 10));
		assertEquals(29, bar.getNextIncrBlock(catalogDAO, 2));
		assertEquals(31, bar.getNextIncrValue(catalogDAO));
	}

	@Test
	public void testRemoveValue() throws PEException {
		long[] testValues = { 5, 10, 20, 50 };
		catalogDAO.refresh(bar);

		AutoIncrementTracker autoIncr = bar.getAutoIncr();

		for (int tv = 0; tv < testValues.length; ++tv) {
			long testValue = testValues[tv];
			autoIncr.removeValue(catalogDAO, testValue);
			for(long incrValue; (incrValue=autoIncr.getNextValue(catalogDAO)) <= testValue;)
				assertTrue("AutoIncrementTracker returned removed value " + testValue, incrValue != testValue);

			// make sure calling remove again results in a no-op and no exception is thrown
			autoIncr.removeValue(catalogDAO, testValue);
		}
	}

	@Test
	public void testRemoveValueStatic() throws PEException {
		long[] testValues = { 5, 10, 20, 50 };
		catalogDAO.refresh(bar);

		AutoIncrementTracker autoIncr = bar.getAutoIncr();

		for (int tv = 0; tv < testValues.length; ++tv) {
			long testValue = testValues[tv];
			AutoIncrementTracker.removeValue(catalogDAO, autoIncr.getId(), testValue);
			for(long incrValue; (incrValue=autoIncr.getNextValue(catalogDAO)) <= testValue;)
				assertTrue("AutoIncrementTracker returned removed value " + testValue, incrValue != testValue);

			// make sure calling remove again results in a no-op and no exception is thrown
			AutoIncrementTracker.removeValue(catalogDAO, autoIncr.getId(), testValue);
		}
	}

	@Ignore 
	@Test 
	public void testConcurrentGetNextId_128() throws Exception {
		doTestConcurrentGetIdBlock(1, 128, 20000);
	}

	@Ignore 
	@Test 
	public void testConcurrentGetBlock() throws Exception {
		doTestConcurrentGetIdBlock(13, 32, 20000);
	}

	static int WARMUP_ITERATIONS = 1000;

	private void doTestConcurrentGetIdBlock(int numIds, int numThreads, int numIterations)
			throws Exception {

		catalogDAO.refresh(foo);

		final ExecutorService executor = Executors .newFixedThreadPool(numThreads);
		final AutoIncrementTracker ait = foo.getAutoIncr();

		System.out.format("AutoIncrementTracker.getIdBlock(%d) with %d threads:%n", numIds, numThreads);

		// create a catalog DAO per thread
		BlockingQueue<CatalogDAO> catalogs = new ArrayBlockingQueue<CatalogDAO>(numThreads);
		for (int i = 0; i < numThreads; i++) {
			catalogs.put(CatalogDAOFactory.newInstance());
		}

		if (WARMUP_ITERATIONS > 0) {
			System.out.println("Warming up... (" + WARMUP_ITERATIONS + ")");
			executor.invokeAll(getTasks(ait.getId(), WARMUP_ITERATIONS, numIds, catalogs));
		}

		System.out.println("Running ... (" + numIterations + ")");
		List<Callable<Long>> incrementTasks = getTasks(ait.getId(), numIterations, numIds, catalogs);
		long start = System.currentTimeMillis();
		List<Future<Long>> results = executor.invokeAll(incrementTasks);
		long elapsed = System.currentTimeMillis() - start;
		double average = elapsed / (double) numIterations;
		System.out.format("  Average time (ms) = %.2f%n", average);

		// cleanup
		executor.shutdown();
		do {
			catalogs.take().close();
		} while (!catalogs.isEmpty());

		AutoIncrementTracker.dumpStats();

		// validate results
		Set<Long> set = new HashSet<Long>();
		for (Future<Long> future : results) {
			set.add(future.get());
		}
		assertEquals(results.size(), set.size());

		// wait for any pending cache update...
		Thread.sleep(500);

		catalogDAO.begin();
		assertEquals((WARMUP_ITERATIONS + numIterations) * numIds + ait.getCacheSize() + 1, ait.readNextValue(catalogDAO));
		catalogDAO.commit();
	}

	private List<Callable<Long>> getTasks(final int tableId,
			final int iterations, final int numIds, final BlockingQueue<CatalogDAO> catalogs) {

		List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(iterations);
		for (int i = 0; i < iterations; i++) {
			Callable<Long> task = new Callable<Long>() {
				@Override
				public Long call() throws Exception {
					CatalogDAO c = catalogs.take();
					try {
						Thread.sleep(57);
						return AutoIncrementTracker.getIdBlock(c, tableId, numIds);
					} finally {
						catalogs.put(c);
					}
				}
			};
			tasks.add(task);
		}
		return tasks;
	}

}
