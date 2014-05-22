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


import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.variable.SchemaVariableConstants;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

// seems to be stable, but will hold off on turning it on for reals
@Test(enabled=false,groups={"NonSmokeTest"})
public class MultithreadTestNG extends SchemaTest {

	private static final boolean haltOnFailure = Boolean.getBoolean("mttest.haltOnFailure");
	
	private static final StorageGroupDDL pg = new StorageGroupDDL("pndt",3,"pg");
 
	private static final PEDDL testDDL =
		new PEDDL("mtdb",pg, 
				"database").withTemplate("mttemp", true).withMTMode(MultitenantMode.ADAPTIVE);
	
	private static final PEDDL concurDDL =
			new PEDDL("concurins",pg,"database");
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(testDDL,concurDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	PortalDBHelperConnectionResource rootConnection;
	
	@BeforeMethod
	public void setupTest() throws Throwable {
		rootConnection = new PortalDBHelperConnectionResource();
		rootConnection.execute(new TemplateBuilder("mttemp")
			.withRequirement("create range block_range (int) persistent group #sg#")
			.withRangeTable(".*block", "block_range", "bid")
			.withRangeTable(".*square", "block_range", "squid")
			.withRangeTable(".*", "block_range", "___mtid")
			.toCreateStatement());
//		rootConnection.execute("alter dve set cache_limit = 0");
		rootConnection.execute("alter dve set statistics_interval=0");
		rootConnection.execute("alter dve set " + SchemaVariableConstants.TABLE_GARBAGE_COLLECTOR_INTERVAL_NAME + " = 1000");
		testDDL.getPersistentGroup().create(rootConnection);
	}

	@AfterMethod
	public void teardownTest() throws Throwable {
		testDDL.destroy(rootConnection);
		rootConnection.disconnect();
		// someone put this in, I have no idea why this is needed
        WorkerGroupFactory.shutdown(PETest.bootHost.getWorkerManager());
		aborted = null;
	}
	
	private volatile Throwable aborted = null;
	
	// so, the purpose of this test is for n threads to execute the same sql (with asserts)
	// and to make sure that they all get the correct results.  To that end, this acts more
	// like SchemaSystemTest - set up a list of actions, then fire them up in one or more
	// threads concurrently.  Each thread gets its own connection resource

	private List<Action> buildActions(boolean alters, int extraShapes) {
		ArrayList<Action> actions = new ArrayList<Action>();
		actions.add(new Action() {

			@Override
			public void execute(ConnectionResource cr) throws Throwable {
				SchemaTest.echo("Begin actions on " + cr.describe());
			}
			
		});
		actions.add(new ProcAction("create table `altest` ( `cola` int not null auto_increment, `module` varchar(64) not null, primary key (`cola`))"));
		actions.add(new ProcAction("insert into altest (module) values ('quick'),('scots'),('rule')"));
		actions.add(new Action() {

			@Override
			public void execute(ConnectionResource cr) throws Throwable {
				cr.execute("insert into altest (module) values ('" + cr.describe() + "')");
				cr.assertResults("/* " + cr.describe() + "*/ select * from altest order by cola limit 4", 
						br(nr,new Integer(1),"quick",
						   nr,new Integer(2),"scots",
						   nr,new Integer(3),"rule",
						   nr,new Integer(4),cr.describe()));
			}
			
		});
		if (extraShapes > 0) {
			char[] letters = new char[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' };
			for(int i = 0; i <= extraShapes; i++) {
				LinkedList<Integer> offsets = new LinkedList<Integer>();
				int current = i;
				while(current != 0) {
					int nc = current / 10;
					int rem = current % 10;
					offsets.addFirst(rem);
					current = nc;
				}
				StringBuffer buf = new StringBuffer();
				buf.append("create table `mtab");
				for(Integer o : offsets) {
					buf.append(letters[o.intValue()]);
				}
				buf.append("` (`id` int, ");
				for(int o = 0; o < offsets.size(); o++) {
					int ov = offsets.get(o).intValue();
					if (o > 0)
						buf.append(", ");
					buf.append("`").append(letters[ov]).append(o).append("` int");
				}
				if (!offsets.isEmpty())
					buf.append(", ");
				buf.append(" primary key (`id`)) comment '").append(i).append("'");
				actions.add(new ProcAction(buf.toString()));
			}
		}
		if (alters) {
			actions.add(new ProcAction("alter table altest add `book` int not null"));
			actions.add(new AssertAction("show columns in altest like 'book'",br(nr,"book","int(11)","NO","",null,"")));
			final Integer zero = new Integer(0);
			actions.add(new AssertAction("select module, book from altest order by cola limit 3",
					br(nr,"quick",zero,nr,"scots",zero,nr,"rule",zero)));
			actions.add(new ProcAction("alter table altest add `bookish` varchar(32), add unique key `bookish` (`bookish`)"));
			//		actions.add(new PrintAction("show columns in altest like 'bookish'"));
			actions.add(new AssertAction("show columns in altest like 'bookish'",br(nr,"bookish","varchar(32)","YES","",null,"")));
			actions.add(new Action() {

				@Override
				public void execute(ConnectionResource cr) throws Throwable {
					cr.assertResults("select module, book, bookish from altest order by cola limit 4",
							br(nr,"quick",zero,null,
									nr,"scots",zero,null,
									nr,"rule",zero,null,
									nr,cr.describe(),zero,null));
				}

			});
			actions.add(new ProcAction("insert into altest (module,bookish,book) values ('one','warren piece',1001), ('two','mo bedick',2002), ('three','catch her in therye',3003)"));
		}
		actions.add(new ProcAction("drop table `altest`"));
		actions.add(new Action() {

			@Override
			public void execute(ConnectionResource cr) throws Throwable {
				SchemaTest.echo("Finished actions on " + cr.describe());
			}
			
		});
		return actions;
	}

	private void runActions(int minimumConnections, int maximumConnections, List<Action> actions, boolean rebuildDB) throws Throwable {
		for(int nconn = minimumConnections; nconn <= maximumConnections; nconn++) {
			runTest(nconn,testDDL,actions,rebuildDB);
			if (aborted != null)
				throw aborted;
		}
	}
	
	@Test(enabled=false)
	public void testCreateDropNoExtrasRebuildDB() throws Throwable {
		List<Action> actions = buildActions(false,0);
		runActions(10,50,actions,true);
	}
	
	@Test(enabled=false)
	public void testCreateAlterDropNoExtrasRebuildDB() throws Throwable {
		List<Action> actions = buildActions(true,0);
		runActions(20,30,actions,true);
	}
	
	@Test(enabled=false)
	public void testCreateDropExtrasRebuildDB() throws Throwable {
		List<Action> actions = buildActions(true,100);
		runActions(10,30,actions,true);
	}
	
	private void runTest(int nconn, PEDDL ddl, List<Action> actions, boolean rebuildDB) throws Throwable {

		System.out.println("in  runTest(nconn=" + nconn + ")");

		if (rebuildDB) {
			rootConnection.execute("drop multitenant database if exists " + testDDL.getDatabaseName());
			rootConnection.execute(testDDL.getCreateDatabaseStatement());
		} else {
			// otherwise, we're going to drop the tenants
			for(int i = 0; i < nconn; i++) {
				String tenantName = "ten" + i;
				rootConnection.execute("drop database if exists " + tenantName); 
			}
		}
		// typically drop tenant/drop database wouldn't happen that fast - simulate that
		try {
			Thread.sleep(5000);
		} catch (InterruptedException ie) {
		}
		
		ArrayList<ConnectionResource> connections = new ArrayList<ConnectionResource>();
		
		// all access is local
		String access = "localhost";
				
		// assume the database is already created - create n users, n tenants, create privs for the tenants on the users
		// create conns for each user
		for(int i = 0; i < nconn; i++) {
			String userName = "mtun" + i;
			removeUser(rootConnection,userName,access);
			rootConnection.execute("create user '" + userName + "'@'" + access + "' identified by '" + userName + "'");
			String tenantName = "ten" + i;
			rootConnection.execute("create tenant " + tenantName + " '" + tenantName + "' on " + ddl.getDatabaseName());
			rootConnection.execute("grant all on " + tenantName + ".* to '" + userName + "'@'" + access + "' identified by '" + userName + "'");
			PortalDBHelperConnectionResource tencon = new PortalDBHelperConnectionResource(userName,userName);
			tencon.execute("use " + tenantName);
			connections.add(tencon);
		}
		
		
		List<ActionThread> threads = new ArrayList<ActionThread>();
		FailureCallback stopper = new StaticFailureCallback(threads);
				
		System.out.println("****** starting " + nconn + " conns ***********");
		
		for(int i = 0; i < connections.size(); i++) {
			ActionThread at = new ActionThread("mttest-thread" + i,connections.get(i),stopper,actions);
			threads.add(at);
		}
		
		LinkedList<ActionThread> total = new LinkedList<ActionThread>(threads);
		runThreads(total);
		
		for(ConnectionResource cr : connections)
			cr.disconnect();
		
		// System.out.println("out runTest(nconn=" + nconn + ")");

	}
	
	private <T extends MultiThreadTestThread> void runThreads(LinkedList<T> threads) {
		for(T mttt : threads)
			mttt.start();
		while(!threads.isEmpty()) {
			for(Iterator<T> iter = threads.iterator(); iter.hasNext();) {
				T current = iter.next();
				try {
					current.join();
					iter.remove();
				} catch(InterruptedException ie) {
					// ignore
				}
			}
		}
	}
	
	private static abstract class Action {
		
		public abstract void execute(ConnectionResource cr) throws Throwable;
	}
	
	private static class ProcAction extends Action {
		
		private String sql;
		
		public ProcAction(String stmt) {
			sql = stmt;
		}

		@Override
		public void execute(ConnectionResource cr) throws Throwable {
			try {
				cr.execute(sql);
			} catch (Throwable t) {
				throw new Throwable("Unexpected exception on " + cr.describe() + " for stmt " + sql,t);
			}
		}
	}
	
	private static class AssertAction extends Action {
		
		private String sql;
		private Object[] expected;
		
		public AssertAction(String stmt, Object[] vals) {
			sql = stmt;
			expected = vals;
		}

		@Override
		public void execute(ConnectionResource cr) throws Throwable {
			try {
				cr.assertResults(sql, expected);
			} catch (Throwable t) {
				throw new Throwable("Unexpected exception on " + cr.describe() + " for stmt " + sql,t);
			}
		}
		
	}
	
	// helpful for debugging
	@SuppressWarnings("unused")
	private static class PrintAction extends Action {

		private String sql;
		
		public PrintAction(String stmt) {
			sql = stmt;
		}
		
		@Override
		public void execute(ConnectionResource cr) throws Throwable {
			SchemaTest.echo(cr.describe() + "(" + sql + ") : " + cr.printResults(sql));
		}
		
	}
	
	interface FailureCallback {
		
		void onException(Throwable t);
		
	}

	private class StaticFailureCallback implements FailureCallback {

		private final List<? extends MultiThreadTestThread> threads;
		
		public StaticFailureCallback(List<? extends MultiThreadTestThread> in) {
			threads = in;
		}
		
		@Override
		public void onException(Throwable t) {
			aborted = t;
			t.printStackTrace();
			if (haltOnFailure)
				Runtime.getRuntime().halt(1);
			// default just stops all threads, then fails the test
			for(MultiThreadTestThread at : threads) {
				at.abort();
			}
			fail(t.getMessage());
		}
		
	}
	
	private static abstract class MultiThreadTestThread extends Thread {
		
		protected final ConnectionResource conn;
		protected final FailureCallback excb;
		protected volatile boolean aborted;

		public MultiThreadTestThread(String name, ConnectionResource conn, FailureCallback cb) {
			super(name);
			this.conn = conn;
			this.excb = cb;
		}

		public void abort() {
			aborted = true;
		}
		
		@Override
		public abstract void run();
		
	}
	
	private static class ActionThread extends MultiThreadTestThread {
		
		final List<Action> actions;
		
		public ActionThread(String name, ConnectionResource connection,
				FailureCallback cb, 
				List<Action> acts) {
			super(name, connection,cb);
			actions = acts;
		}
		
		@Override
		public void run() {
			for(Action act : actions) try {
				if (aborted)
					return;
				act.execute(conn);
			} catch (Throwable t) {
				excb.onException(t);
				// exceptions generally mean the states messed up, no need to continue
				return;
			}
		}
	}

	@Test(enabled=false)
	public void testConcurrentInserts() throws Throwable {
		concurDDL.create(rootConnection);
		rootConnection.execute("create range concrange (int) persistent group " + testDDL.getPersistentGroup().getName());
		rootConnection.execute("use concurins");
		rootConnection.execute("create table ttab (`id` int not null auto_increment, `sid` int, `who` varchar(32), primary key (`id`)) range distribute on (`sid`) using concrange");
		List<ProxyConnectionResource> conns = new ArrayList<ProxyConnectionResource>();
		for(int i = 0; i < 10; i++) {
			ProxyConnectionResource pcr = new ProxyConnectionResource();
			pcr.execute("use concurins");
			conns.add(pcr);
		}
		LinkedList<ConcurrentInsertThread> threads = new LinkedList<ConcurrentInsertThread>();
		FailureCallback fcb = new StaticFailureCallback(threads);
		for(int i = 0; i < conns.size(); i++) {			
			threads.add(new ConcurrentInsertThread("ccr" + i, conns.get(i), fcb));
		}
		runThreads(threads);
		for(ProxyConnectionResource pcr : conns)
			pcr.disconnect();
		System.out.println(rootConnection.printResults("select id, count(*) from ttab group by id having count(*) > 1"));
		rootConnection.execute("drop database concurins");
	}

	private static class ConcurrentInsertThread extends MultiThreadTestThread {
				
		public ConcurrentInsertThread(String name, ConnectionResource conn, FailureCallback fcb) {
			super(name,conn,fcb);
		}
		
		@Override
		public void run() {
			String myName = getName();
			Random rand = new Random();
			for(int i = 1; i < 1000; i++) try {
				if (aborted) return;
				try {
					sleep(rand.nextInt(10));
				} catch (InterruptedException ie) {
					// ignore
				}
				conn.execute("insert into ttab (`sid`, `who`) values (" + i + ", '" + myName + "')");
			} catch (Throwable t) {
				excb.onException(t);
				return;
			}
		}
	}
	
}
