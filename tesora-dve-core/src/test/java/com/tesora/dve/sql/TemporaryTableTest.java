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


import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class TemporaryTableTest extends SchemaTest {

	private static final ProjectDDL sysDDL = new PEDDL("sysdb",
			new StorageGroupDDL("sys", 2, "sysg"), "schema");

	private static final ProjectDDL encDDL = new PEDDL("encdb",
			new StorageGroupDDL("enc",2,"encg"), "database");
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(sysDDL, encDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		sysDDL.create(pcr);
		pcr.disconnect();
		pcr = null;
	}

	@SuppressWarnings("resource")
	@Test
	public void testCreation() throws Throwable {
		final DBHelperConnectionResource conn1 = new PortalDBHelperConnectionResource();
		final DBHelperConnectionResource conn2 = new PortalDBHelperConnectionResource();
		
		try {
			conn1.execute("use " + sysDDL.getDatabaseName());
			conn1.execute("start transaction");
			conn1.execute("create temporary table foo (id int, fid int) broadcast distribute");
			conn1.execute("insert into foo (id, fid) values (1,1),(2,2),(3,3)");
			conn1.assertResults("select * from information_schema.temporary_tables",
					br(nr,3,"sysdb","foo","InnoDB"));
			conn1.execute("commit");
			conn1.assertResults("select * from foo",
					br(nr,1,1,
					   nr,2,2,
					   nr,3,3));
			conn1.assertResults("select * from information_schema.global_temporary_tables", 
					br(nr,"LocalhostCoordinationServices:1",3,"sysdb","foo","InnoDB"));

			conn2.assertResults("select * from information_schema.temporary_tables",br());
			conn2.assertResults("select * from information_schema.global_temporary_tables",
					br(nr,"LocalhostCoordinationServices:1",3,"sysdb","foo","InnoDB"));

			new ExpectedSqlErrorTester() {
				@Override
				public void test() throws Throwable {
					conn2.execute("use " + sysDDL.getDatabaseName());
					conn2.assertResults("show tables", br());
					conn2.execute("select * from foo");

				}
			}.assertError(SQLException.class,
						MySQLErrors.missingTableFormatter,
						"Table 'sysdb.foo' doesn't exist");
		} finally {
			conn1.disconnect();
			conn2.disconnect();
		}
		
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testInfoSchema() throws Throwable {
		DBHelperConnectionResource conn1 = null;
		DBHelperConnectionResource conn2 = null;
		
		String kern = 
				"select column_name, ordinal_position from information_schema.columns where table_schema = '" 
				+ sysDDL.getDatabaseName() + "' and table_name = '%s'";
		
		try {
			conn1 = new PortalDBHelperConnectionResource();
			conn2 = new PortalDBHelperConnectionResource();
			conn1.execute("use " + sysDDL.getDatabaseName());
			conn2.execute("use " + sysDDL.getDatabaseName());
			
			conn1.execute("create table narrow (id int , fid int, primary key (id)) broadcast distribute");
			conn1.assertResults(String.format(kern,"narrow"),
					br(nr,"id",1,
					   nr,"fid",2));
			conn2.assertResults(String.format(kern,"narrow"),
					br(nr,"id",1,
					   nr,"fid",2));
			conn1.execute("create temporary table narrow (id int auto_increment, fid int, sid int, primary key (id), unique key(fid), key (sid)) broadcast distribute");
			conn1.assertResults(String.format(kern,"narrow"),
					br(nr,"id",1,
					   nr,"fid",2));
	
			conn1.assertResults("describe narrow",
					br(nr,"id","int(11)","NO","PRI",null,"auto_increment",
					   nr,"fid","int(11)","YES","UNI",null,"",
					   nr,"sid","int(11)","YES","MUL",null,""));
			conn2.assertResults("describe narrow",
					br(nr,"id","int(11)","NO","PRI",null,"",
					   nr,"fid","int(11)","YES","",null,""));
			
			conn1.assertResults("show columns in narrow like 'fid'",
					br(nr,"fid","int(11)","YES","UNI",null,""));
			
			conn1.assertResults("show keys in narrow",
					br(nr,"narrow",1L,"PRIMARY",1,"id","A",-1,4,"","NO","BTREE","","",
					   nr,"narrow",1L,"fid",1,"fid","A",-1,4,"","YES","BTREE","","",
					   nr,"narrow",0L,"sid",1,"sid","A",-1,4,"","YES","BTREE","",""));
			conn2.assertResults("show keys in narrow",
					br(nr,"narrow",0L,"PRIMARY",1,"id","A",-1,null,null,"","BTREE","",""));
			conn1.execute("drop table narrow");
			conn1.execute("drop table narrow");
		} finally {
			if (conn1 != null)
				conn1.disconnect();
			if (conn2 != null)
				conn2.disconnect();
		}

	}
	
	@Test
	public void testDMLPlanning() throws Throwable {
		try (final DBHelperConnectionResource conn = new PortalDBHelperConnectionResource()) {
			conn.execute("use " + sysDDL.getDatabaseName());
			conn.execute("create table src (id int, fid int, primary key (id)) random distribute");
			conn.execute("insert into src (id, fid) values (1,1),(2,2),(3,3),(4,4),(5,5)");
			conn.execute("create table rt (id int, fid int, primary key (id)) random distribute");
			conn.execute("create temporary table targ (id int, fid int, primary key (id)) broadcast distribute");
			conn.execute("insert into targ (id, fid) select * from src");
			conn.assertResults("select count(*) from targ",br(nr,5L));
			conn.execute("insert into rt (id, fid) select * from targ");
			conn.assertResults("select count(*) from rt",br(nr,5L));
			conn.execute("drop table src");
			conn.execute("drop table rt");
			conn.execute("drop table targ");
		} 
	}
	
	@Test
	public void testLifecycle() throws Throwable {
		try (final DBHelperConnectionResource conn = new PortalDBHelperConnectionResource()) {
			conn.execute("use " + sysDDL.getDatabaseName());
			conn.execute("create temporary table targ (id int, fid int, primary key (id)) static distribute on (id)");
			conn.execute("insert into targ (id,fid) values (1,1),(2,2),(3,3),(4,4),(5,5)");
			conn.assertResults("select * from information_schema.temporary_tables",
					br(nr,ignore,"sysdb","targ","InnoDB"));
			conn.assertResults("describe targ",
					br(nr,"id","int(11)","NO","PRI",null,"",
					   nr,"fid","int(11)","YES","",null,""));
			conn.execute("alter table targ add `sid` int default '22'");
			conn.assertResults("describe targ",
					br(nr,"id","int(11)","NO","PRI",null,"",
					   nr,"fid","int(11)","YES","",null,"",
					   nr,"sid","int(11)","YES","","'22'",""));
			conn.execute("drop table targ");
			conn.assertResults("select * from information_schema.temporary_tables",br());
			new ExpectedSqlErrorTester() {
				@Override
				public void test() throws Throwable {
					conn.execute("select * from targ");
				}
			}.assertError(SQLException.class, MySQLErrors.missingTableFormatter,
						"Table 'sysdb.targ' doesn't exist");
		}
	}
	
	@Test
	public void testHiding() throws Throwable {
		try (final DBHelperConnectionResource conn = new PortalDBHelperConnectionResource()) {
			conn.execute("use " + sysDDL.getDatabaseName());
			
			String tdef = "create temporary table targ (id int auto_increment, fid int, primary key (id)) static distribute on (id)";
			String pdef = "create table targ (id int auto_increment, booyeah int, primary key (id), unique key (booyeah))";
			
			Object[] pdesc = 
					br(nr,"id","int(11)","NO","PRI",null,"auto_increment",
					   nr,"booyeah","int(11)","YES","",null,"");
			Object[] tdesc =
					br(nr,"id","int(11)","NO","PRI",null,"auto_increment",
					   nr,"fid","int(11)","YES","",null,"");
			Object[] showTabs = br(nr,"targ");
			
			conn.execute(pdef);
			conn.assertResults("describe targ",pdesc);
			conn.assertResults("show tables",showTabs);

			// should fail - no temporary table
			new ExpectedSqlErrorTester() {
				@Override
				public void test() throws Throwable {
					conn.execute("drop temporary table targ"); // should fail - doesn't exist
				}
			}.assertError(SQLException.class, MySQLErrors.unknownTableFormatter,
						"Unknown table 'targ'");
			conn.execute("drop table targ"); // succeeds
			conn.assertResults("show tables",br());
			
			conn.execute(tdef);
			conn.assertResults("show tables",br());
			conn.assertResults("describe targ",tdesc);
			conn.assertResults("select * from information_schema.temporary_tables",
					br(nr,ignore,"sysdb","targ","InnoDB"));
			conn.execute(pdef);
			conn.assertResults("show tables",showTabs);
			conn.assertResults("describe targ",tdesc);
			conn.assertResults("select column_name from information_schema.columns where table_schema = '" + sysDDL.getDatabaseName() + "' and table_name = 'targ'",
					br(nr,"id",
					   nr,"booyeah"));
			
			conn.execute("drop table targ");
			conn.assertResults("show tables",showTabs);
			conn.assertResults("describe targ",pdesc);
			conn.execute(tdef);
			conn.assertResults("describe targ",tdesc);
			conn.execute("drop temporary table targ");
			conn.assertResults("show tables",showTabs);
			conn.assertResults("describe targ",pdesc);
			
			conn.execute("drop table targ"); // cleanup
		}
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testConnectionClose() throws Throwable {
		DBHelperConnectionResource conn1 = null;
		DBHelperConnectionResource conn2 = null;
		
		String globalQuery = 
				"select * from information_schema.global_temporary_tables";
		
		try {
			conn1 = new PortalDBHelperConnectionResource();
			conn2 = new PortalDBHelperConnectionResource();
			conn1.execute("use " + sysDDL.getDatabaseName());
			conn2.execute("use " + sysDDL.getDatabaseName());
			
			conn1.execute("create temporary table tt1 (id int, fid int, primary key (id))");
			conn1.execute("create table pt1 (id int, fid int, primary key (id))");
			conn1.execute("insert into pt1 (id, fid) values (1,1),(2,2),(3,3),(4,4),(5,5)");
			conn1.execute("start transaction");
			conn1.execute("insert into tt1 (id, fid) select id, fid from pt1");
			conn1.assertResults("select count(*) from tt1",br(nr,5L));
			conn2.assertResults(globalQuery, 
					br(nr,"LocalhostCoordinationServices:1",ignore,"sysdb","tt1","InnoDB"));
			conn1.execute("commit");
			conn1.disconnect();
			conn1 = null;
			// apparently the client can quit before the server is entirely done - so sleep a few seconds
			try {
				Thread.sleep(3000);
			} catch (InterruptedException ie) {
				// ignore
			}
			conn2.assertResults(globalQuery,br());
			conn2.execute("drop table pt1");
		} finally {
			if (conn1 != null) try {
				conn1.disconnect();
			} catch (Throwable t) {
				// ignore
			}
			if (conn2 != null) try {
				conn2.disconnect();
			} catch (Throwable t) {
				// ignore
			}
		}
		
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testEnclosingDrop() throws Throwable {
		DBHelperConnectionResource conn1 = null;
		DBHelperConnectionResource conn2 = null;

		try {
			conn1 = new PortalDBHelperConnectionResource();
			conn2 = new PortalDBHelperConnectionResource();
			
			encDDL.create(conn2);
			
			conn1.execute("use " + encDDL.getDatabaseName());
			
			conn1.execute("create temporary table tt1 (id int, fid int, primary key (id))");
			conn1.execute("insert into tt1 (id,fid) values (1,1),(2,2),(3,3)");
			
			// this should work
			conn2.execute("drop database " + encDDL.getDatabaseName());

			conn1.assertResults(String.format("select count(*) from %s.tt1",
					encDDL.getDatabaseName()),
					br(nr,3L));
			
			DropStorageGroupThread hangingOut = 
					new DropStorageGroupThread(conn2,
							"drop persistent group " + encDDL.getPersistentGroup().getName());
			hangingOut.start();

			try {
				Thread.sleep(3000);
			} catch (InterruptedException ie) {
				// ignore
			}
			
			if (!hangingOut.isIssued())
				fail("Apparently unable to start the hangout thread");
			
			conn1.execute("drop temporary table " + encDDL.getDatabaseName() + ".tt1");
			
			// sleep for a few seconds
			try {
				Thread.sleep(3000);
			} catch (InterruptedException ie) {
				// ignore
			}
			
			if (hangingOut.getOops() != null)
				throw hangingOut.getOops();
			
			if (hangingOut.isAlive())
				fail("Apparently still waiting even though the temp table is toast");

		} finally {
			try {
				conn1.disconnect();
			} catch (Throwable t) {
				// ignore
			}
			try {
				conn2.disconnect();
			} catch (Throwable t) {
				// ignore
			}

		}
		
	}

	private static class DropStorageGroupThread extends Thread {
		
		DBHelperConnectionResource connection;
		Throwable oops = null;
		String sql;
		boolean issuedDrop = false;
		
		public DropStorageGroupThread(DBHelperConnectionResource c,
				String sql) {
			super("dsgt");
			connection = c;
			this.sql = sql;
		}
		
		public boolean isIssued() {
			return issuedDrop;
		}
		
		public Throwable getOops() {
			return oops;
		}
		
		public void run() {
			try {
				issuedDrop = true;
				connection.execute(sql);
			} catch (Throwable t) {
				oops = t;
			}
		}
		
	}
	
	@Test
	public void testTemporaryCTA() throws Throwable {
		DBHelperConnectionResource conn1 = null;
		DBHelperConnectionResource conn2 = null;
		
		String globalQuery = 
				"select * from information_schema.global_temporary_tables";
		
		try {
			conn1 = new PortalDBHelperConnectionResource();
			conn2 = new PortalDBHelperConnectionResource();
			conn1.execute("use " + sysDDL.getDatabaseName());
			conn2.execute("use " + sysDDL.getDatabaseName());

			conn1.execute("create table ctasrca (id int, fid int, av varchar(8), primary key (id)) random distribute");
			conn1.execute("create range ctarange (int) persistent group " + sysDDL.getPersistentGroup().getName());
			conn1.execute("create table ctasrcb (id int, fid int, bv varchar(8), primary key(id)) range distribute on (id) using ctarange");
			conn1.execute("insert into ctasrca (id,fid,av) values (1,1,'aone'),(2,2,'atwo'),(3,3,'athree'),(4,4,'afour'),(5,5,'afive')");
			conn1.execute("insert into ctasrcb (id,fid,bv) values (2,2,'btwo'),(4,4,'bfour')");
			
			conn2.assertResults(globalQuery, br());
			
			conn1.execute("create temporary table ctatarg (primary key (id)) "
					+"range distribute on (id) using ctarange "
					+"select a.id as id, a.av as lv, b.bv as rv from ctasrca a inner join ctasrcb b on a.id = b.id");
			
			conn2.assertResults(globalQuery, 
					br(nr,"LocalhostCoordinationServices:1",ignore,"sysdb","ctatarg","InnoDB"));
						
			conn1.assertResults("select count(*) from ctatarg",br(nr,2L));
			
			conn1.disconnect();
			conn1 = null;

			conn2.execute("drop table ctasrca, ctasrcb");			
			
			conn2.assertResults(globalQuery, br());
		} finally {
			if (conn1 != null) try {
				conn1.disconnect();
				conn1 = null;
			} catch (Throwable t) {
				// ignore
			}
			if (conn2 != null) try {
				conn2.disconnect();
				conn2 = null;
			} catch (Throwable t) {
				// ignore
			}
		}
	}
}
