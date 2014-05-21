// OS_STATUS: public
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

import java.util.ArrayList;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class BugsMirrorTest extends SchemaMirrorTest {

	private static final ProjectDDL checkDDL =
			new PEDDL("checkdb",
					new StorageGroupDDL("check",1,"checkg"),
					"schema");
	private static final ProjectDDL multiDDL =
			new PEDDL("checkdb",
					new StorageGroupDDL("sys",3,"sysg"),
					"schema");

	private static final NativeDDL nativeDDL =
		new NativeDDL("cdb");
		
	@Override
	protected ProjectDDL getSingleDDL() {
		return checkDDL;
	}
		
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@Override
	protected ProjectDDL getMultiDDL() {
		return multiDDL;
	}
	

	
	@BeforeClass
	public static void setup() throws Throwable {
		setup(multiDDL, checkDDL, nativeDDL, Collections.<MirrorTest> emptyList());
	}

	@Test
	public void testDateFunctionsWithIntervals() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorProc("create table pe825 (id int, ut timestamp not null, primary key (id)) /*#dve broadcast distribute */"));
		out.add(new StatementMirrorProc("set time_zone = 'SYSTEM'"));
		out.add(new StatementMirrorProc("insert into pe825 values "
				+"(1,'2012-09-18 04:30:00'),(2,'2012-09-20 04:30:00'),"
				+"(3,'2012-09-18 09:00:30'),(4,'2012-09-22 13:25:32')"));
		out.add(new StatementMirrorFun("select * from pe825 order by id"));
		String[] exprs = new String[] { "22","1","0","13" };
		String[] units = new String[] { "microsecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year" };
		String[] funcs = new String[] {	"adddate(ut,%s)", "date_add(ut,%s)", "ut + %s", "ut - %s", "date_sub(ut,%s)", "subdate(ut,%s)" };
		for(String e : exprs) {
			for(String u : units) {
				for(int i = 1; i < 5; i++) {
					String istr = "interval " + e + " " + u;
					for(String f : funcs) {
						out.add(new StatementMirrorFun("select " + String.format(f,istr) + " as tv from pe825 where id = " + i));
					}
				}
			}
		}	
		out.add(new StatementMirrorProc("drop table pe825"));
		ListOfPairs<TestResource,TestResource> config = new ListOfPairs<TestResource,TestResource>();
		config.add(nativeResource,checkResource);
		runTest(out, config, false);
	}
	
	@Test
	public void testMetadataGeneration() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorProc("create table md1 (id int, fid int, primary key (id))"));
//		out.add(new StatementMirrorProc("create table md2 (id int, fid int, primary key (id))"));
		out.add(new StatementMirrorFun("select * from md1"));
		out.add(new StatementMirrorFun("select l.* from md1 l"));
		out.add(new StatementMirrorFun("select id as i, fid as fi from md1"));
		out.add(new StatementMirrorFun("select l.id as i, l.fid as fi from md1 l"));
		out.add(new StatementMirrorFun("select id + 1, fid * 22 from md1"));
		out.add(new StatementMirrorFun("select l.id + 1, l.fid * 22 from md1 l"));
		// add more cases here
		out.add(new StatementMirrorProc("drop table md1"));
//		out.add(new StatementMirrorProc("drop table md2"));
		ListOfPairs<TestResource,TestResource> config = new ListOfPairs<TestResource,TestResource>();
		config.add(nativeResource,sysResource);
		runTest(out, config, false);		
	}
	
	@Test
	public void testPE849() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		final String[] tabNames = new String[] { "BC", "AR", "RA", "ST" };
		final String[] distDecls = new String[] { "broadcast distribute", "random distribute", "range distribute on (id) using pe849r", "static distribute on (id)" };
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (!mr.getDDL().isNative()) 
					mr.getConnection().execute("create range pe849r (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				String body = " (id int, aid int(4) zerofill, bid int zerofill, primary key (id)) ";
				ResourceResponse rr = null;
				for(int i = 0; i < tabNames.length; i++) {
					StringBuffer buf = new StringBuffer();
					buf.append("create table ").append(tabNames[i]).append(body).append("/*#dve ").append(distDecls[i]).append(" */");
					rr = mr.getConnection().execute(buf.toString());
				}
				return rr;
			}
			
		});
		for(String tn : tabNames) {
			out.add(new StatementMirrorProc("insert into " + tn + " values (1,1,1),(2,2,2),(3,3,3),(4,4,4)"));
		}
		// this works without any mods, because this just streams the unordered select (with the casts) onto a temp site for
		// the order by
		for(String tn : tabNames) {
			out.add(new StatementMirrorFun("select id, cast(aid as char(4)), cast(bid as char(10)) from " + tn + " order by id"));
		}
		int nonrand[] = new int[] { 0, 2, 3 };
		for(int i = 0; i < nonrand.length; i++) {
			out.add(new StatementMirrorFun("select cast(l.aid as char(4)), cast(r.bid as char(10)) from AR l inner join " + tabNames[nonrand[i]] + " r on l.id = r.id order by l.id"));
		}
		for(String tn : tabNames) {
			out.add(new StatementMirrorProc("drop table " + tn));
		}
		ListOfPairs<TestResource,TestResource> config = new ListOfPairs<TestResource,TestResource>();
		config.add(nativeResource,sysResource);
		runTest(out, config, false);
	}

	@Test
	public void testPE1078() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		final String[] tabNames = new String[] { "BC", "AR", "RA", "ST" };
		final String[] distDecls = new String[] { "broadcast distribute", "random distribute", "range distribute on (id) using pe1078r", "static distribute on (id)" };
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (!mr.getDDL().isNative()) 
					mr.getConnection().execute("create range pe1078r (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				String body = " (id int, primary key (id)) ";
				ResourceResponse rr = null;
				for(int i = 0; i < tabNames.length; i++) {
					StringBuffer buf = new StringBuffer();
					buf.append("create table ").append(tabNames[i]).append(body).append("/*#dve ").append(distDecls[i]).append(" */");
					rr = mr.getConnection().execute(buf.toString());
				}
				return rr;
			}
			
		});
		for(String tn : tabNames) {
			out.add(new StatementMirrorProc("insert into " + tn + " values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16)"));
		}
		// first start with something small
		for(String tn : tabNames) {
			out.add(new StatementMirrorProc("select id from " + tn + " order by id limit 1 offset 2"));
		}
		// now choose a bunch of different limits and offsets; our max limit should be 5 and our max offset should be 15
		for(String tn : tabNames) {
			for(int lf = 1; lf < 3; lf++) {
				for(int of = 1; of < 10; of++) {
					out.add(new StatementMirrorProc("select id from " + tn + " order by id limit " + (lf * 2) + " offset " + (of * 2)));
				}
			}
		}
		for(String tn : tabNames) {
			out.add(new StatementMirrorProc("drop table " + tn));
		}
		ListOfPairs<TestResource,TestResource> config = new ListOfPairs<TestResource,TestResource>();
		config.add(nativeResource,sysResource);
		runTest(out, config, false);
		
	}
	
	@Test
	public void testPE342() throws Throwable {
		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("DROP TABLE IF EXISTS pe342"));
		tests.add(new StatementMirrorProc("CREATE TABLE pe342 (id int NOT NULL, st varchar(255) NOT NULL, u int(11) NOT NULL) ENGINE=MyISAM /*#dve broadcast distribute */"));
		tests.add(new StatementMirrorProc("INSERT INTO pe342 VALUES (1, 'a', 1), (2, 'A', 1), (3, 'aa', 1), (4, 'AA', 1), (5, 'a', 1), (6, 'aaa', 0), (7, 'BBB', 0)"));
		tests.add(new StatementMirrorFun("SELECT BINARY st FROM pe342 ORDER BY id ASC"));
		tests.add(new StatementMirrorFun("SELECT IF(u = 1, binary st, st) s FROM pe342 ORDER BY s"));
		runTest(tests);
	}

	@Test
	public void testPE768Modify() throws Throwable {
		final String testQuery = "select * from pe768_modify order by a asc";

		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("drop table if exists pe768_modify"));
		tests.add(new StatementMirrorProc("create table pe768_modify (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, PRIMARY KEY(a))"));
		tests.add(new StatementMirrorProc("insert into pe768_modify values (1, 2, 3), (4, 5, 6), (7, 8, 9)"));

		tests.add(new StatementMirrorProc("alter table pe768_modify modify a INT NOT NULL first"));
		tests.add(new StatementMirrorFun(testQuery));
		tests.add(new StatementMirrorProc("alter table pe768_modify modify a INT NOT NULL after c"));
		tests.add(new StatementMirrorFun(testQuery));
		tests.add(new StatementMirrorProc("alter table pe768_modify modify c INT NOT NULL first"));
		tests.add(new StatementMirrorFun(testQuery));

		runTest(tests);
	}

	@Test
	public void testPE768Change() throws Throwable {
		final String testQuery = "select * from pe768_change order by a asc";

		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("drop table if exists pe768_change"));
		tests.add(new StatementMirrorProc("create table pe768_change (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, PRIMARY KEY(a))"));
		tests.add(new StatementMirrorProc("insert into pe768_change values (1, 2, 3), (4, 5, 6), (7, 8, 9)"));

		tests.add(new StatementMirrorProc("alter table pe768_change change a d INT NOT NULL first"));
		tests.add(new StatementMirrorFun("select * from pe768_change order by d asc"));
		tests.add(new StatementMirrorProc("alter table pe768_change change d a INT NOT NULL after c"));
		tests.add(new StatementMirrorFun(testQuery));
		tests.add(new StatementMirrorProc("alter table pe768_change change c c INT NOT NULL first"));
		tests.add(new StatementMirrorFun(testQuery));

		runTest(tests);
	}
	
	@Test
	public void testPE768() throws Throwable {
		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();

		tests.add(new StatementMirrorProc("drop table if exists pe768"));
		tests.add(new StatementMirrorProc("create table pe768 (col1 int not null auto_increment primary key, col2 varchar(30) not null, col3 varchar (20) not null, col4 varchar(4) not null, col5 enum('PENDING', 'ACTIVE', 'DISABLED') not null, col6 int not null, to_be_deleted int)"));
		tests.add(new StatementMirrorProc("insert into pe768 values (2, 4, 3, 5, 'PENDING', 1, 7)"));

		tests.add(new StatementMirrorProc("alter table pe768 add column col4_5 varchar(20) not null after col4, add column col7 varchar(30) not null after col5, add column col8 datetime not null, drop column to_be_deleted, change column col2 fourth varchar(30) not null after col3, modify column col6 int not null first"));
		tests.add(new StatementMirrorFun("select * from pe768 order by col1 asc"));

		runTest(tests);
	}

	@Test
	public void testPE1480Add() throws Throwable {
		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();

		tests.add(new StatementMirrorProc("drop table if exists pe1480_add"));
		tests.add(new StatementMirrorProc("create table pe1480_add (a INT, b INT, c INT)"));
		tests.add(new StatementMirrorProc("insert into pe1480_add values (1, 2, 3)"));

		tests.add(new StatementMirrorProc("alter table pe1480_add add d INT after b, add e INT after d, add f INT after e"));
		tests.add(new StatementMirrorFun("select * from pe1480_add"));

		runTest(tests);
	}

	@Test
	public void testPE1480Modify() throws Throwable {
		final ArrayList<MirrorTest> schema = new ArrayList<MirrorTest>();
		schema.add(new StatementMirrorProc("drop table if exists pe1480_modify"));
		schema.add(new StatementMirrorProc("create table pe1480_modify (a INT, b INT, c INT)"));
		schema.add(new StatementMirrorProc("insert into pe1480_modify values (1, 2, 3)"));

		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();

		tests.addAll(schema);
		tests.add(new StatementMirrorProc("alter table pe1480_modify change c d INT, add e INT after d"));
		tests.add(new StatementMirrorFun("select * from pe1480_modify"));

		runTest(tests);

		tests.clear();

		tests.addAll(schema);
		tests.add(new StatementMirrorProc("alter table pe1480_modify change c d INT, modify b INT after d; "));
		tests.add(new StatementMirrorFun("select * from pe1480_modify"));

		runTest(tests);
	}

	@Test
	public void testPE1500() throws Throwable {
		final ArrayList<MirrorTest> schema = new ArrayList<MirrorTest>();
		schema.add(new StatementMirrorProc("DROP TABLE IF EXISTS pe1500a"));
		schema.add(new StatementMirrorProc("DROP TABLE IF EXISTS pe1500b"));
		schema.add(new StatementMirrorProc("DROP TABLE IF EXISTS pe1500c"));
		schema.add(new StatementMirrorProc("CREATE TABLE `pe1500a` ("
				+ "`id` bigint(20) NOT NULL AUTO_INCREMENT,"
				+ "`scr` double NOT NULL,"
				+ "`tfs` datetime DEFAULT NULL,"
				+ "`tls` datetime DEFAULT NULL,"
				+ "`citlv` bit(1) NOT NULL,"
				+ "PRIMARY KEY (`id`)"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=6019 DEFAULT CHARSET=utf8"));
		schema.add(new StatementMirrorProc("CREATE TABLE `pe1500b` ("
				+ "`id` bigint(20) NOT NULL AUTO_INCREMENT,"
				+ "`wri` bigint(20) DEFAULT NULL,"
				+ "PRIMARY KEY (`id`),"
				+ "KEY `FK_l80pbag6lyu32l644tw1ce4io` (`wri`)"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8"));
		schema.add(new StatementMirrorProc("CREATE TABLE `pe1500c` ("
				+ "`id` bigint(20) NOT NULL AUTO_INCREMENT,"
				+ "`wri` bigint(20) DEFAULT NULL,"
				+ "PRIMARY KEY (`id`),"
				+ "UNIQUE KEY `FKF0B8DE827B382519` (`wri`)"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8"));
		schema.add(new StatementMirrorProc(
				"INSERT INTO `pe1500a` VALUES (5567,1,'2014-02-14 23:03:13','2014-02-14 23:03:13','\0')"));

		final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();

		tests.addAll(schema);

		tests.add(new StatementMirrorFun("select "
				+ "this_.citlv as citlv3_248_24_, "
				+ "this_.scr as scr6_248_24_, "
				+ "this_.tfs as tfs7_248_24_, "
				+ "this_.tls as tls8_248_24_ "
				+ "from pe1500a this_ "
				+ "left outer join pe1500b fr18 on this_.id=fr18.wri "
				+ "left outer join pe1500c wr25 on this_.id=wr25.wri "
				+ "where (this_.id in (5567))"));

		runTest(tests);
	}
}
