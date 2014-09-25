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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class SimpleContainerTest extends SchemaTest {

	private static final StorageGroupDDL theStorageGroup = 
		new StorageGroupDDL("check",2,"checkg");

	private static final ProjectDDL testDDL =
		new PEDDL("checkdb",theStorageGroup,"schema");

	@BeforeClass
	public static void setupProject() throws Throwable {
		PETest.projectSetup(testDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ProxyConnectionResource rootConn = new ProxyConnectionResource();
		rootConn.close();
	}

	private ProxyConnectionResource conn;
	
	@Before
	public void setup() throws Throwable {
		conn = new ProxyConnectionResource();
		testDDL.create(conn);
		conn.execute("create range cont_range (int) persistent group " + testDDL.getPersistentGroup().getName());
		conn.execute("create container testcont persistent group " + testDDL.getPersistentGroup().getName() + " range distribute using cont_range");
//		conn.execute("create table A (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont");
//		conn.execute("create table B (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont");
	}

	private void declareBaseTable() throws Throwable {
		conn.execute("create table bt (`id` int, `junk` varchar(32), `unused` varchar(32), primary key (id)) discriminate on (id,junk) using container testcont");		
	}
	
	@After
	public void teardown() throws Throwable {
		testDDL.destroy(conn);
		conn.disconnect();
		conn = null;
	}
	
	@Test
	public void testA() throws Throwable {
		declareBaseTable();
		conn.execute("using container testcont (global)");
		conn.execute("insert into bt values (1,'one'),(2,'two')");
		conn.assertResults("show container_tenants",
				br(nr,"testcont","id:1,junk:'one'",getIgnore(),
				   nr,"testcont","id:2,junk:'two'",getIgnore()));
		conn.execute("create table A (`id` int, `junk` varchar(32), primary key (`id`)) container distribute testcont");
		conn.assertResults("describe A",br(nr,"id","int(11)","NO","PRI",null,"",
				nr,"junk","varchar(32)","YES","",null,""));
		conn.execute("set @@dve_metadata_extensions = 1");
		conn.assertResults("describe A",br(nr,"id","int(11)","NO","PRI",null,"",
				nr,"junk","varchar(32)","YES","",null,"",
				nr,"___mtid","int(10) unsigned","NO","",null,""));
		conn.execute("drop database " + testDDL.getDatabaseName());
		conn.execute("drop container testcont");
	}
	
	@Test
	public void testB() throws Throwable {
		declareBaseTable();
		conn.execute("using container testcont (global)");
		conn.execute("insert into bt values (1, 'one')");
		conn.assertResults("show container_tenants",br(nr,"testcont","id:1,junk:'one'",getIgnore()));
		conn.execute("insert into bt values (2, 'two')");
		conn.assertResults("show container_tenants like '%two%'", br(nr,"testcont","id:2,junk:'two'",getIgnore()));
		conn.execute("using container testcont (1, 'one')");
		String errorMessage = "Internal error: Inserts into base table `bt` for container testcont must be done when in the global container context";
		try {
			conn.execute("insert into bt values (3, 'three')");
			fail("should not be able to insert into base table as a nonglobal container tenant");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter, errorMessage);
		}
		conn.disconnect();
		conn.connect();
		conn.execute("use " + testDDL.getDatabaseName());
		try {
			conn.execute("insert into bt values (3, 'three')");
			fail("should not be able to insert into base table in the null container tenant");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter, errorMessage);
		}
	}
	
	@Test
	public void testC() throws Throwable {
		// conn.execute("alter dve set plan_cache_limit = 0");
		declareBaseTable();
		conn.execute("using container testcont (global)");
		conn.execute("insert into bt values (1, 'one')");
		conn.execute("insert into bt values (2, 'two')");
		conn.execute("using container testcont (1, 'one')");		
		conn.execute("create table A (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont");
		conn.execute("create table B (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont");
		conn.execute("insert into A values (1, 'one')");
		conn.assertResults("select * from A where id = 1", br(nr,new Integer(1),"one"));
		conn.execute("insert into B values (1, 'uno')");
		conn.assertResults("select * from B where id = 1", br(nr, new Integer(1), "uno"));
		conn.assertResults("select a.junk, b.junk from A a, B b where a.id = b.id",br(nr,"one","uno"));
		conn.execute("using container testcont (junk='two', id=2)");
		conn.execute("insert into A values (2, 'two')");
		conn.assertResults("select * from A where id = 2", br(nr,new Integer(2), "two"));
		conn.execute("using container testcont (global)");
		conn.assertResults("select * from A order by id", br(nr,new Integer(1), "one", nr, new Integer(2), "two"));
		try {
			conn.execute("drop container testcont");
			fail("should not be able to drop nonempty container");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter, 
					"Internal error: Unable to drop container testcont due to existing tables");
		}
		// I shouldn't be able to drop the base table out of the container if the container has other nonbase tables
		// this is because if I could, then I could no longer access those tables. 
		try {
			conn.execute("drop table bt");
			fail("should not be able to drop base table when other tables exist");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: Unable to drop table `bt` because it is the base table to container testcont which is not empty");
		}
		conn.execute("drop table A");
		conn.execute("drop table B");
		conn.assertResults("show container_tenants",
				br(nr,"testcont","id:1,junk:'one'",getIgnore(),
				   nr,"testcont","id:2,junk:'two'",getIgnore()));
		conn.execute("drop table bt");
		conn.assertResults("show container_tenants", br());
		try {
			conn.execute("drop range cont_range");
			fail("should not be able to drop range for container in use");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: Unable to drop range cont_range because used by container testcont");
		}
	}
	
	@Test
	public void testD() throws Throwable {
		declareBaseTable();
		conn.execute("using container testcont (global)");
		conn.execute("insert into bt values (1, 'one')");
		conn.execute("insert into bt values (2, 'two')");
		conn.execute("using container testcont (1, 'one')");		
		conn.execute("create table A (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont");
		conn.execute("create table B (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont");
		conn.disconnect();
		conn.connect();
		conn.execute("use " + testDDL.getDatabaseName());
		// this should fail
		try {
			conn.execute("insert into A values (1,'one')");
			fail("shouldn't be able to insert into CMT with no context specified");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: Inserts into table `A` for container testcont must be done when in a specific container context");
		}
		// shouldn't be able to do it in the global context either, I don't think
		conn.execute("using container testcont (global)");
		try {
			conn.execute("insert into A values (1,'one')");
			fail("shouldn't be able to insert into CMT with nonglobal context specified");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: Inserts into table `A` for container testcont must be done when in a specific container context");
		}
	}
	
	@Test
	public void testE() throws Throwable {
		conn.execute("drop database " + testDDL.getDatabaseName());
		conn.execute("drop container testcont");
		conn.execute("drop range cont_range");
		conn.execute("create container droptest persistent group " + testDDL.getPersistentGroup().getName() + " random distribute");
		try {
			conn.execute("drop persistent group " + testDDL.getPersistentGroup().getName());
			fail("shouldn't be able to drop a persistent group from under an existing container");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: Unable to drop persistent group checkg because used by container droptest");
		}
	}
	
	@Test
	public void testF() throws Throwable {
		declareBaseTable();
		conn.execute("using container testcont (global)");
		conn.execute("insert into bt values (1, 'one')");
		conn.execute("insert into bt values (2, 'two')");
		// cannot alter the columns
		try {
			conn.execute("alter table bt drop junk");
			fail("should not be able to drop disc column");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: Illegal alter on container base table `bt` discriminant column `junk`");
		}
		conn.execute("alter table bt alter column `unused` set default 'deadbeef'");
		try {
			// this should not
			conn.execute("update bt set id = 20 where unused = 'whatever'");
			fail("should not be able to update disc columns");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.invalidDiscriminantUpdateFormatter,
					"id","bt");
		}
		// this should work
		conn.execute("update bt set unused = 'whatever' where id = 1");

		// should also do change def, but will have to pull that in
		try {
			conn.execute("delete from bt where unused = 'whatever'");
			fail("should not be able to range delete from base table");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.invalidContainerDeleteFormatter,
					"bt");
		}
		// this does work, however
		conn.execute("delete from bt where id = 2 and junk = 'two'");
		conn.assertResults("show container_tenants", br(nr,"testcont","id:1,junk:'one'",getIgnore()));
	}	
	
	// containers and fks
	@Test
	public void testFKs() throws Throwable {
		declareBaseTable();
		conn.execute("using container testcont (global)");
		conn.execute("insert into bt values (1, 'one')");
		conn.execute("insert into bt values (2, 'two')");
		conn.execute("create table A (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont");
		conn.execute("create table B (`id` int, `junk` varchar(32), `fid` int, primary key (id), foreign key (fid) references A (id)) container distribute testcont");
		conn.assertResults("select * from information_schema.table_constraints where table_name != 'bt'",
				br(nr,"def","checkdb","PRIMARY","checkdb","A","PRIMARY KEY",
				   nr,"def","checkdb","PRIMARY","checkdb","B","PRIMARY KEY",
				   nr,"def","checkdb","B_ibfk_1","checkdb","B","FOREIGN KEY"));
		String dbn = testDDL.getDatabaseName();
		conn.assertResults("select * from information_schema.referential_constraints",br(nr,"def", dbn, ignore, "def", dbn, "RESTRICT","RESTRICT","B","A"));
		try {
			conn.execute("create table C (`id` int, `sid` int, primary key (id), foreign key (sid) references B (id)) random distribute");
			fail("should not be able to create a table with a noncolocated fk");
		} catch (SchemaException se) {
			assertErrorInfo(se,MySQLErrors.internalFormatter,
					"Internal error: Invalid foreign key C.C_ibfk_1: table C is not colocated with B");
		}
		conn.execute("set foreign_key_checks = 0");
		conn.execute("create table D (`id` int, `sid` int, primary key (id), foreign key tfk (sid) references E (id)) container distribute testcont");
		Object[] results = br(nr,"def",dbn,ignore,"def",dbn,"RESTRICT","RESTRICT","B","A",
							  nr,"def",dbn,ignore,"def",dbn,"RESTRICT","RESTRICT","D","E");
		conn.assertResults("select * from information_schema.referential_constraints",results);
		Object one = new Integer(1);
//		System.out.println(conn.printResults("select * from information_schema.key_column_usage"));
		conn.assertResults("select * from information_schema.key_column_usage",
				br(nr,"def","def","checkdb","bt","id",one,null,null,null,
				   nr,"def","def","checkdb","A","id",one,null,null,null,
				   nr,"def","def","checkdb","B","id",one,null,null,null,
				   nr,"def","def","checkdb","B","fid",one,"checkdb","A","id",
				   nr,"def","def","checkdb","D","id",one,null,null,null,
				   nr,"def","def","checkdb","D","sid",one,"checkdb","E","id"));
		try {
			conn.execute("create table E (`id` int, `junk` varchar(32), primary key (`id`)) random distribute");
			fail("should not be able to create a table which is the target of a noncolocated fk");
		} catch (SchemaException se) {
			assertErrorInfo(se,MySQLErrors.internalFormatter,
					"Internal error: Invalid foreign key D.D_ibfk_1: table D is not colocated with E");
		}
		conn.execute("create table E (`id` int, `junk` varchar(32), primary key (`id`)) container distribute testcont");
		conn.assertResults("select * from information_schema.referential_constraints",results);
		conn.assertResults("select * from information_schema.key_column_usage",
				br(nr,"def","def","checkdb","bt","id",one,null,null,null,
				   nr,"def","def","checkdb","A","id",one,null,null,null,
				   nr,"def","def","checkdb","B","id",one,null,null,null,
				   nr,"def","def","checkdb","B","fid",one,"checkdb","A","id",
				   nr,"def","def","checkdb","D","id",one,null,null,null,
				   nr,"def","def","checkdb","D","sid",one,"checkdb","E","id",
				   nr,"def","def","checkdb","E","id",one,null,null,null));
	}
	
	@Test
	public void testGlobalScope() throws Throwable {
		conn.execute("create table a (id int, col2 varchar(10)) discriminate on (id) using container testcont");
		conn.execute("create table b (id int, tbla_id int, tble_id int) container distribute testcont");
		conn.execute("create table c (id int, tblb_id int) container distribute testcont");
		conn.execute("create table d (dkey varchar(10), type int) container distribute testcont");
		conn.execute("create table e (id int, desc varchar(10)) persistent group checkg broadcast distribute");
		conn.execute("using container testcont (global)");
		conn.execute("insert into a values (10,'ten')");
		conn.execute("using container testcont (10)");
	}
	
	@Test
	public void testPE468() throws Throwable {
		conn.execute("create range dis_range (int) persistent group " + testDDL.getPersistentGroup().getName());
		conn.execute("create container discont persistent group " + testDDL.getPersistentGroup().getName() + " range distribute using dis_range");
		conn.execute("create table cont_bt (id int, who varchar(10)) discriminate on (id) using container testcont");
		conn.execute("create table dis_bt (id int, who varchar(10)) discriminate on (id) using container discont");
		conn.execute("create table cont_cmt (id int, who varchar(10)) container distribute testcont");
		conn.execute("create table dis_cmt (id int, who varchar(10)) container distribute discont");
		conn.execute("using container testcont (global)");
		conn.execute("insert into cont_bt values (10, 'ten'), (11,'eleven'),(12,'twelve'),(13,'thirteen')");
		conn.execute("insert into dis_bt values (15, 'fifteen'), (16, 'sixteen'), (17, 'seventeen'), (18, 'eighteen')");
		conn.execute("using container testcont (10)");
		conn.execute("insert into cont_cmt values (1, 'cont_one')");
		conn.execute("using container discont (15)");
		conn.execute("insert into dis_cmt values (1, 'dis_one')");
		conn.execute("using container discont (16)");
		try {
			conn.execute("insert into cont_cmt values (1, 'cont_onep')");
		} catch (Exception e) {
			SchemaTest.assertSchemaException(e, "Invalid container context");
		}
		// conn.execute("insert into dis_cmt values (1, 'dis_onep')");
		// System.out.println(conn.printResults("select id, who, ___mtid, @dve_sitename, 'cont_cmt' from cont_cmt"));
		// System.out.println(conn.printResults("select id, who, ___mtid, @dve_sitename, 'dis_cmt' from dis_cmt"));
	}

	@Test
	public void testPE465() throws Throwable {
		conn.execute("create table a (id int, col2 varchar(10)) discriminate on (id) using container testcont");
		try {
			conn.execute("create table b (id int, col2 varchar(10)) discriminate on (id) using container testcont");
			fail("Expected exception on setting second table as base table in container");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: Cannot set table 'b' as a base table because container 'testcont' already has a base table.");
		}
	}

	@Test
	public void testPE469() throws Throwable {
		declareBaseTable();
		conn.execute("using container testcont (global)");
		conn.execute("insert into bt values (1, 'global')");
		conn.execute("using container testcont (1, 'global')");		
		conn.execute("create table A (`id` int, `junk` varchar(32), primary key (id)) container distribute testcont");
		conn.execute("insert into A values (1, 'global')");
		conn.execute("using container testcont (junk='global', id=1)");
		conn.assertResults("select * from A where id = 1", br(nr,new Integer(1), "global"));
		
		try {
			conn.execute("using container testcont (global, junk='global', id=1)");
			fail("Cannot specify GLOBAL context and discriminators");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: GLOBAL context cannot be specified with container discriminant.");
		}

		try {
			conn.execute("using container testcont (global, junk='global', null, id=1)");
			fail("Cannot specify GLOBAL context and discriminators");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: GLOBAL context cannot be specified with container discriminant.");
		}
		
		try {
			conn.execute("using container testcont (null, junk='global', id=1)");
			fail("Cannot specify NULL context and discriminators");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: NULL context cannot be specified with container discriminant.");
		}
		
		try {
			conn.execute("using container testcont (junk='global', null, global, id=1)");
			fail("Cannot specify NULL context and discriminators");
		} catch (SchemaException e) {
			assertErrorInfo(e,MySQLErrors.internalFormatter,
					"Internal error: NULL context cannot be specified with container discriminant.");
		}
	}

	@Test
	public void testPE524OrderedConstraints() throws Throwable {		
		conn.execute("create table `parent` ( `id` int, `name` varchar(255), PRIMARY key (`id`) "
				+ ") ENGINE=InnoDb DEFAULT CHARSET= utf8 COLLATE=utf8_unicode_ci "
				+ "container distribute testcont " );
		conn.execute("create table `child` ( `id` int, `p_id` int, key `key_c` (`id`), key `key_p` (`p_id`) "
				+ ", CONSTRAINT `key_p` FOREIGN KEY (`p_id`) REFERENCES `parent` (`id`) "
				+ ") ENGINE=InnoDb DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci "
				+ "container distribute testcont ");
	}

	@Test
	public void testPE524OutOfOrderConstraints() throws Throwable {
		conn.execute("SET FOREIGN_KEY_CHECKS=0");
		conn.execute("create table `child` (`id` int, `p_id` int, key `key_c` (`id`), key `key_p` (`p_id`) "
				+ ", CONSTRAINT `cfkey_p` FOREIGN KEY (`p_id`) REFERENCES `parent` (`id`) "
				+ ") ENGINE=InnoDb DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci "
				+ "container distribute testcont ");
		conn.execute("create table `parent` (`id` int, `name` varchar(255), PRIMARY key (`id`) "
				+ ") ENGINE=InnoDb DEFAULT CHARSET= utf8 COLLATE=utf8_unicode_ci "
				+ "container distribute testcont " );
	}

}
