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
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.*;

import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

//SMG:disable to see if everything else is OK.
@Ignore
public class KillTest extends SchemaTest {

	private static final ProjectDDL checkDDL = new PEDDL("checkdb",
			new StorageGroupDDL("check", 2, "checkg"), "schema");

	private static final String MSG_QUERY_INTERRUPTED = "Query execution was interrupted";

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	private DBHelperConnectionResource nativeConn;
	private DBHelperConnectionResource clientConn;
	private ExecutorService executor;

	@Before
	public void connect() throws Throwable {
		nativeConn = new DBHelperConnectionResource();
		clientConn = new PortalDBHelperConnectionResource();
		checkDDL.create(clientConn);
		executor = Executors.newSingleThreadExecutor();
	}

	@After
	public void disconnect() throws Throwable {
		nativeConn.disconnect();
		nativeConn = null;
		checkDDL.destroy(clientConn);
		clientConn.disconnect();
		clientConn = null;
		executor.shutdownNow();
		executor.awaitTermination(10, TimeUnit.SECONDS);
		executor = null;
	}

	@Test
	public void testInvalidConnectionId() throws Throwable {
		try {
			clientConn.execute("KILL QUERY 12345");
		} catch (SQLException e) {
			assertTrue(e.getMessage().contains("Unknown thread id: 12345"));
		}
	}

	@Test
	public void testKillQuery() throws Throwable {
		if (Boolean.getBoolean("disableTestKillQuery"))
			return;
		clientConn.execute("CREATE TABLE `killtest` (`id` int NOT NULL)");
		clientConn.execute("INSERT INTO checkdb.killtest (`id`) VALUES (123)");

		try (final DBHelperConnectionResource client2 = new PortalDBHelperConnectionResource()) {

			ExpectedFailureTask blockedQuery = new ExpectedFailureTask() {
				@Override
				protected void execute() throws Throwable {
					try {
						client2.execute("SELECT id FROM checkdb.killtest");
					} catch (java.sql.SQLException e) {
						assertTrue("Wrong exception, expected '" + MSG_QUERY_INTERRUPTED + "', got: " + e.getMessage(),
								e.getMessage().contains(MSG_QUERY_INTERRUPTED));
					}
				}
			};

			try {
				// lock table to hang SELECT
				nativeConn.execute("LOCK TABLES check0_checkdb.killtest WRITE");
				Future<Void> future = executor.submit(blockedQuery);
				waitForQueryToBlock();

				// kill the hung query
				clientConn.execute("KILL QUERY " + getLastThreadId(clientConn));
				future.get(20, TimeUnit.SECONDS);

				// release locks, query again -- confirm connection is still live
				nativeConn.execute("UNLOCK TABLES");
				future = executor.submit(new ExpectedFailureTask() {
					@Override
					protected void execute() throws Throwable {
						assertEquals(client2.execute("SELECT id FROM checkdb.killtest").getResults().size(), 1);
					}
				});
				future.get(20, TimeUnit.SECONDS);
			} finally {
				nativeConn.execute("UNLOCK TABLES");
			}
		}
	}

	@Test
	public void testKillIdleConnection() throws Throwable {
		try (DBHelperConnectionResource client2 = new PortalDBHelperConnectionResource()) {
			client2.execute("SELECT version()");
			clientConn.execute("KILL CONNECTION " + getLastThreadId(clientConn));
			try {
				// confirm connection was killed
				client2.execute("SELECT version()");
			} catch (Exception e) {
				assertTrue("Wrong exception", findExceptionsOfType(java.io.EOFException.class, e).size() > 0);
			}
		}
	}

	@Test
	public void testKillBlockedConnection() throws Throwable {
		clientConn.execute("CREATE TABLE `killtest` (`id` int NOT NULL)");
		clientConn.execute("INSERT INTO checkdb.killtest (`id`) VALUES (123)");

		try (final DBHelperConnectionResource client2 = new PortalDBHelperConnectionResource()) {

			ExpectedFailureTask blockedQuery = new ExpectedFailureTask() {
				@Override
				protected void execute() throws Throwable {
					try {
						client2.execute("SELECT id FROM checkdb.killtest");
					} catch (java.sql.SQLException e) {
						assertTrue(e.getMessage().contains(MSG_QUERY_INTERRUPTED));
					}
				}
			};

			try {
				nativeConn.execute("LOCK TABLES check0_checkdb.killtest WRITE");
				Future<Void> future = executor.submit(blockedQuery);
				waitForQueryToBlock();
				clientConn.execute("KILL " /* CONNECTION is optional here */+ getLastThreadId(clientConn));
				future.get(20, TimeUnit.SECONDS);
			} finally {
				nativeConn.execute("UNLOCK TABLES");
			}

			try {
				// confirm connection was killed
				client2.execute("SELECT id FROM checkdb.killtest");
			} catch (Exception e) {
				assertTrue("Not the expected exception", findExceptionsOfType(java.io.EOFException.class, e).size() > 0);
			}
		}
	}

	private long getLastThreadId(DBHelperConnectionResource conn) throws Throwable {
		List<ResultRow> results = conn.execute("show processlist").getResults();
		ResultRow lastRow = results.get(results.size() - 1);
		ResultColumn col = lastRow.getResultColumn(1);
		return ((Long) col.getColumnValue()).intValue();
	}

	private void waitForQueryToBlock() throws Throwable {
		boolean blocked;
		do {
			blocked = checkProcessState(nativeConn, "Waiting for table metadata lock")
					|| checkProcessState(nativeConn, "Waiting for schema metadata lock");
		} while (!blocked);
	}

	private boolean checkProcessState(DBHelperConnectionResource conn, String state) throws Throwable {
		List<ResultRow> results = conn.execute("show processlist").getResults();
		for (ResultRow row : results) {
			ResultColumn col = row.getResultColumn(7);
			if (state.equals(col.getColumnValue())) {
				return true;
			}
		}
		return false;

	}

	private static abstract class ExpectedFailureTask implements Callable<Void> {

		@Override
		public Void call() throws Exception {
			try {
				execute();
			} catch (Exception | Error e) {
				throw e;
			} catch (Throwable t) {
				throw new Exception(t);
			}
			return null;
		}

		// do something and check for expected exception
		protected abstract void execute() throws Throwable;
	}

}
