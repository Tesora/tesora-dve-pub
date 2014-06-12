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
import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

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

	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(sysDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		
		ProxyConnectionResource pcr = new ProxyConnectionResource();
		sysDDL.create(pcr);
		pcr.disconnect();
		pcr = null;
	}

	@SuppressWarnings("resource")
	@Test
	public void testCreation() throws Throwable {
		DBHelperConnectionResource conn1 = null;
		DBHelperConnectionResource conn2 = null;
		
		try {
			conn1 = new PortalDBHelperConnectionResource();
			conn1.execute("use " + sysDDL.getDatabaseName());
			conn1.execute("start transaction");
			conn1.execute("create temporary table foo (id int, fid int) broadcast distribute");
			conn1.execute("insert into foo (id, fid) values (1,1),(2,2),(3,3)");
//			System.out.println(conn1.printResults("select * from information_schema.temporary_tables"));
			conn1.assertResults("select * from information_schema.temporary_tables",
					br(nr,3,"sysdb","foo","InnoDB"));
			conn1.execute("commit");
			conn1.assertResults("select * from foo",
					br(nr,1,1,
					   nr,2,2,
					   nr,3,3));
			conn1.assertResults("select * from information_schema.global_temporary_tables", 
					br(nr,"LocalhostCoordinationServices:1",3,"sysdb","foo","InnoDB"));
			conn2 = new PortalDBHelperConnectionResource();
			conn2.assertResults("select * from information_schema.temporary_tables",br());
			conn2.assertResults("select * from information_schema.global_temporary_tables",
					br(nr,"LocalhostCoordinationServices:1",3,"sysdb","foo","InnoDB"));
			try {
				conn2.execute("use " + sysDDL.getDatabaseName());
				conn2.assertResults("show tables", br());
				conn2.execute("select * from foo");
			} catch (SQLException sqle) {
				assertEquals(sqle.getMessage(), "SchemaException: No such Table: sysdb.foo");
			}
		} finally {
			if (conn1 != null)
				conn1.disconnect();
			if (conn2 != null)
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
	
			// so the difference in output here I think has to do with the reloading from the catalog
			// vs never persisting at all...will have to chase this down
//			System.out.println(conn1.printResults("describe narrow"));
			conn1.assertResults("describe narrow",
					br(nr,"id","int(11)","NO","PRI",null,"auto_increment",
					   nr,"fid","int(11)","YES","UNI",null,"",
					   nr,"sid","int(11)","YES","MUL",null,""));
//			System.out.println(conn2.printResults("describe narrow"));
			conn2.assertResults("describe narrow",
					br(nr,"id","int(11)","NO","PRI",null,"",
					   nr,"fid","int(11)","YES","",null,""));
//			System.out.println(conn1.printResults("show columns in narrow like 'fid'"));
			
			conn1.assertResults("show columns in narrow like 'fid'",
					br(nr,"fid","int(11)","YES","UNI",null,""));
			
//			System.out.println(conn1.printResults("show keys in narrow"));
			conn1.assertResults("show keys in narrow",
					br(nr,"narrow",1L,"PRIMARY",1,"id","A",-1,4,"","NO","BTREE","","",
					   nr,"narrow",1L,"fid",1,"fid","A",-1,4,"","YES","BTREE","","",
					   nr,"narrow",0L,"sid",1,"sid","A",-1,4,"","YES","BTREE","",""));
//			System.out.println(conn2.printResults("show keys in narrow"));
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
			conn.execute("insert into rt (id, fid) select * from targ");
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
//			System.out.println(conn.printResults("describe targ"));
			conn.assertResults("describe targ",
					br(nr,"id","int(11)","NO","PRI",null,"",
					   nr,"fid","int(11)","YES","",null,""));
			conn.execute("alter table targ add `sid` int default '22'");
//			System.out.println(conn.printResults("describe targ"));
			conn.assertResults("describe targ",
					br(nr,"id","int(11)","NO","PRI",null,"",
					   nr,"fid","int(11)","YES","",null,"",
					   nr,"sid","int(11)","YES","","'22'",""));
			conn.execute("drop table targ");
			conn.assertResults("select * from information_schema.temporary_tables",br());
			try {
				conn.execute("select * from targ");
			} catch (SQLException sqle) {
				assertEquals(sqle.getMessage(), "SchemaException: No such Table: sysdb.targ");
			}
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
//			System.out.println(conn.printResults("describe targ"));
			conn.assertResults("describe targ",pdesc);
//			System.out.println(conn.printResults("show tables")); 
			conn.assertResults("show tables",showTabs);
			try {
				conn.execute("drop temporary table targ"); // should fail - doesn't exist
				fail("should fail - no temporary table");
			} catch (SQLException sqle) {
				assertEquals(sqle.getMessage(),"SchemaException: No such temporary table: 'targ'");
			}
			conn.execute("drop table targ"); // succeeds
//			System.out.println(conn.printResults("show tables")); // []
			conn.assertResults("show tables",br());
			
			conn.execute(tdef);
//			System.out.println(conn.printResults("show tables")); // []
			conn.assertResults("show tables",br());
//			System.out.println(conn.printResults("describe targ")); // tdef
			conn.assertResults("describe targ",tdesc);
//			System.out.println(conn.printResults("select * from information_schema.temporary_tables"));  // tdef
			conn.assertResults("select * from information_schema.temporary_tables",
					br(nr,ignore,"sysdb","targ","InnoDB"));
			conn.execute(pdef);
//			System.out.println(conn.printResults("show tables")); // [targ]
			conn.assertResults("show tables",showTabs);
//			System.out.println(conn.printResults("describe targ")); // tdef
			conn.assertResults("describe targ",tdesc);
//			System.out.println(conn.printResults("select column_name from information_schema.columns where table_name = 'targ'")); // pdef
			conn.assertResults("select column_name from information_schema.columns where table_schema = '" + sysDDL.getDatabaseName() + "' and table_name = 'targ'",
					br(nr,"id",
					   nr,"booyeah"));
			
			conn.execute("drop table targ");
//			System.out.println(conn.printResults("show tables")); // [targ]
			conn.assertResults("show tables",showTabs);
//			System.out.println(conn.printResults("describe targ")); // pdef
			conn.assertResults("describe targ",pdesc);
			conn.execute(tdef);
//			System.out.println(conn.printResults("describe targ")); // tdef
			conn.assertResults("describe targ",tdesc);
			conn.execute("drop temporary table targ");
//			System.out.println(conn.printResults("show tables")); // [targ]
			conn.assertResults("show tables",showTabs);
//			System.out.println(conn.printResults("describe targ")); // pdef
			conn.assertResults("describe targ",pdesc);
			
			conn.execute("drop table targ"); // cleanup
		}
	}
	
}
