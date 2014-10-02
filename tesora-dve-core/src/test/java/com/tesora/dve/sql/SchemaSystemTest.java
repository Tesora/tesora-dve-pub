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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.JdbcConnectionResourceResponse;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;

// as the name suggests, this is a system test.  because ddl execution is expensive, we'll
// just have a single test - the test is meant to simulate actual usage.
public class SchemaSystemTest extends SchemaTest {

	private static final int SITES = 5;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,2,"sysg"),
				"schema");
	private static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",1,"checkg"),
				"schema");
	private static final StorageGroupDDL checkGroup =
		new StorageGroupDDL("echeck",1,"echeckg");
	private static final StorageGroupDDL sysGroup =
		new StorageGroupDDL("esys",SITES,2,"esysg");

	
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(new StorageGroupDDL[] { checkGroup, sysGroup },sysDDL,checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	@Test
	public void simulate() throws Throwable {
		try (PortalDBHelperConnectionResource sysconn = new PortalDBHelperConnectionResource();
				PortalDBHelperConnectionResource checkconn = new PortalDBHelperConnectionResource()) {

			TestResource smr = new TestResource(sysconn, sysDDL);
			TestResource cmr = new TestResource(checkconn, checkDDL);

			sysDDL.create(smr);
			checkDDL.create(cmr);
			checkGroup.create(cmr);
			sysGroup.create(smr);
			try {
				List<MirrorTest> tests = buildTests();
				for(MirrorTest mt : tests)
					mt.execute(cmr, smr);
			} catch (Exception e) {
                e.printStackTrace();
            } finally {
                // sysddl.destroy(smr);
				// checkddl.destroy(cmr);
			}
		}
	}
	
	// naming scheme for tables: <prefix> <length> <dist model> <autoinc>
	// where length is s(short) or l(long)
	// model is one of b(roadcast), s(tatic), r(andom), ran(g)e
	// autoinc is t(rue) or f(alse)
	// prefix is some letter, like A	
	private List<MirrorTest> buildTests() {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorFun(false,"select @@version_comment limit 1"));
		addInsertTests(tests);
		addParserTests(tests);
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				ConnectionResource conn = mr.getConnection();
				conn.execute("create table `Asbf` (`id` int not null, name varchar(50) default 'text') broadcast distribute");
				SchemaContext pc = mr.getContext();
				if (pc.getOptions() == null)
					pc.setOptions(ParserOptions.NONE);
				TableInstance ti = pc.getCurrentPEDatabase().getSchema().buildInstance(pc,new UnqualifiedName("Asbf"),null);
				assertNotNull(ti);
				PETable tab = ti.getAbstractTable().asTable();
				assertEquals(tab.getDistributionVector(pc).getModel().getPersistentName(), BroadcastDistributionModel.MODEL_NAME);
				ResourceResponse er = conn.execute("create table if not exists `Bsst` (`id` int not null auto_increment, `desc` varchar(50) default 'fragged', primary key (`id`) ) static distribute on (`id`)");
				pc = mr.getContext();
				if (pc.getOptions() == null)
					pc.setOptions(ParserOptions.NONE);
				ti = pc.getCurrentPEDatabase().getSchema().buildInstance(pc, new UnqualifiedName("Bsst"),null);
				assertNotNull(ti);
				tab = ti.getAbstractTable().asTable();
				DistributionVector dv = tab.getDistributionVector(pc);
				assertEquals(dv.getColumns(pc).size(),1);
				assertEquals(dv.getModel().getPersistentName(), StaticDistributionModel.MODEL_NAME);
				return er;
			}
		});
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				PortalDBHelperConnectionResource conn = (PortalDBHelperConnectionResource) mr.getConnection();
				JdbcConnectionResourceResponse response = 
					(JdbcConnectionResourceResponse) conn.execute("insert into Bsst (desc) values('the little dog is sleeping')");
				assertEquals(1, response.getLastInsertId());
				response = (JdbcConnectionResourceResponse) conn.execute("insert into Bsst () values ()");
				assertEquals(2, response.getLastInsertId());
				response = (JdbcConnectionResourceResponse) conn.execute("insert into Bsst (desc) values ('the left handed programmer'),('is very frustrated'),('by how slow one hand is')");
                //See PE-1568.  DBHelper's getLastInsertId() actually returns the first generated row ID, not the last.
				assertEquals(3, response.getLastInsertId());
				conn.assertResults("select * from Bsst where id = 4",br(nr,new Integer(4), "is very frustrated"));
				response = (JdbcConnectionResourceResponse) conn.execute("insert into Asbf (id, name) values (1, 'the ear is healing well'),(2, 'the finger not so well'),(3,'the bank account not at all')");
				return response;
			}
		});
		tests.add(new StatementMirrorFun("select name from Asbf where id = 1"));
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				ConnectionResource conn = mr.getConnection();
				ResourceResponse er = 
					conn.assertResults("select * from Bsst order by desc",
						br(
						nr,new Integer(5), "by how slow one hand is",
						nr,new Integer(2), "fragged",
						nr,new Integer(4), "is very frustrated",
						nr,new Integer(3), "the left handed programmer",
						nr,new Integer(1), "the little dog is sleeping"));
				return er;
			}			
		});
		tests.add(new StatementMirrorFun("select * from Bsst order by desc"));
		tests.add(new StatementMirrorFun("select * from Bsst where id between 2 and 5 order by desc"));
		tests.add(new StatementMirrorProc(
				"drop table if exists dne",
				"create table `dne` (`id` int not null)",
				"insert into dne (`id`) values (1), (2)"
		)); 
		tests.add(new StatementMirrorFun("select * from dne order by id asc"));
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				ConnectionResource conn = mr.getConnection();
				ResourceResponse er = conn.execute("drop table if exists dne");
				SchemaContext pc = mr.getContext();
				assertNull(pc.getCurrentDatabase().getSchema().buildInstance(pc,new UnqualifiedName("dne"),null));
				return er;
			}
		});
		tests.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				return mr.getConnection().execute("SHOW PROCESSLIST");
			}
			
		});
		
		addLeftyTest(tests);
		return tests;
	}

	private void addLeftyTest(List<MirrorTest> tests) {
		tests.add(new MirrorProc() {

			@SuppressWarnings("synthetic-access")
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				ConnectionResource conn = mr.getConnection();
				String dbname = mr.getDDL().getDatabaseName();
				String leftyrange = "r" + dbname + "lefty";
				String orange = "or" + dbname + "lefty";
				String titleSG = null;
				if (mr.getDDL() == checkDDL)
					titleSG = checkGroup.getName();
				else
					titleSG = sysGroup.getName();
				conn.execute("create range " + leftyrange + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				conn.execute("create range " + orange + " (int) persistent group " + titleSG);
				conn.execute(
						"create table `titles` (" 
						+ "`id` int unsigned not null, "
						+ "`name` varchar(50) not null) " 
						+ " persistent group " + titleSG
						+ " range distribute on (`id`) using " + orange);
				conn.execute(
						"create table `states` (" 
						+ "`id` int unsigned not null, "
						+ "`name` varchar(50) not null, " 
						+ "`tag` varchar(50), "
						+ "primary key (`id`)) "
						+ "range distribute on (`id`) using " + leftyrange);
				conn.execute(
						"create table `laws` ("
						+ "`id` int unsigned not null, "
						+ "`state_id` int unsigned not null, "
						+ "`title_id` int unsigned not null, "
						+ "`status` varchar(16) not null default 'unpublished', "
						+ "`version` int unsigned not null, "
						+ "`law` varchar(100), "
						+ "primary key (`id`)) "
						+ "range distribute on (`id`) using " + leftyrange);
				conn.execute(
						"insert into `states` (`id`, `name`, `tag`) values (12,'Iowa','iowa'), " +
						" (17, 'Utah', 'utah'), (37, 'Ohio', 'ohio'), (50, 'Maine', 'maine'), " +
						" (11, 'Texas', 'texas'), (42, 'Idaho', 'idaho')");
				// 1,2,3,11
				conn.execute(
						"insert into `titles` (`id`, `name`) values "
						+ " (3, 'Filing Spouse Title'),  (1, 'Court Clerks Title'),"
						+ " (2, 'Court name'), (11, 'Primary Documents')");
				int lids[] = new int[] { 86, 87, 88, 93, 450, 451, 452, 457, 61, 62, 63, 67, 111, 112, 113, 117, 476, 477, 478, 482, 311, 312, 313, 318 };
				String laws[] = new String[] { "'District Clerks Office'",
						"'In the District Court of Counting County'",
						"'Petitioner.  The Petitioner is the spouse who initiates'",
						"'Petition for Divorce and Decree of Divorce'"
				};
				int sids[] = new int[] { 11, 12, 17, 37, 42, 50 };
				int tids[] = new int[] { 1, 2, 3, 11 };
				int halfway = lids.length / 2;
				ArrayList<String> pre = new ArrayList<String>();
				ArrayList<String> post = new ArrayList<String>();
				for(int i = 0; i < lids.length; i++) {
					String line = "( " + lids[i] + ", " + sids[i / 4] + ", " + tids[i % 4] + ", 1, " + laws[i % 4] + ")";
					if (i <= halfway)
						pre.add(line);
					else
						post.add(line);
				}
				String prefix = "insert into `laws` (`id`, `state_id`, `title_id`, `version`, `law`) values ";
				conn.execute(prefix + Functional.join(pre, ", "));
				String gens = mr.getDDL().getPersistentGroup().getAddGenerations();
				if (gens != null) {
//					conn.disconnect();
//					conn.connect();
					conn.execute("use " + mr.getDDL().getDatabaseName());
					// execute any holdbacks
					conn.execute(gens);
				}
				conn.execute(prefix + Functional.join(post, ", "));
				conn.execute("CREATE INDEX lindex on laws (`law`)");
				return null;
			}
			
		});
		tests.add(new StatementMirrorFun(true, "select l.* from laws l, states s where s.id = 37 and l.state_id = s.id order by l.id desc"));
		tests.add(new StatementMirrorFun(true, "select l.* from laws l, titles t where t.name like 'Court%' and l.title_id = t.id order by l.state_id asc"));
		tests.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Exception {
//				mr.getConnection().execute("update titles set id = 500 where name like '%Title%'");
				return null;
			}

		});
	}
	
	private void addInsertTests(List<MirrorTest> tests) {
		tests.add(new StatementMirrorProc(
				"create table ita (`id` int not null auto_increment, `junky` varchar(25) not null default 'defjunk', `crappy` int not null) static distribute on (`id`)",
				"create table itb (`id` int not null, `crappy` varchar(25)) Engine=InnoDB static distribute on (`id`)",
				"insert into ita (`junky`,`crappy`) values ('a',1),('b',2),('c',3), ('d',5)",
				"insert into ita (`crappy`) values (8), (13)"
		));
		tests.add(new MirrorProc("here") {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				ConnectionResource conn = mr.getConnection();
				conn.execute("set @orig_sql_mode = @@sql_mode");
				conn.execute("set sql_mode='strict_trans_tables'");
//TOPERF
// now that this test uses JDBC we can't see the relevant error message
// we should re-visit this test when proper error handling is implemented
//				try {
//					conn.execute("insert into ita (`junky`) select b.crappy from itb b");
//					fail("should complain about missing value");
//				} catch (PEException e) {
//					assertSchemaException(e,"Missing projection value for non-nullable column");
//				}
				conn.execute("set sql_mode = @orig_sql_mode");
//				try {
//					conn.execute("insert into ita (`id`, `crappy`) select b.id, b.crappy, b.crappy from itb b");
//					fail("should complain about too wide projection");
//				} catch (PEException e) {
//					assertSchemaException(e,"Column list not same size as projection");
//				}
//				try {
//					conn.execute("insert into ita (`id`, `crappy`) select b.crappy from itb b");
//					fail("should complain about too narrow projection");
//				} catch (PEException e) {
//					assertSchemaException(e,"Column list not same size as projection");
//				}
				return conn.execute("insert into itb (`id`, `crappy`) select 2 * a.id, a.junky from ita a order by a.id desc");
			}
			
		});
		tests.add(new StatementMirrorFun("select * from itb order by id"));
		// the redist test seems to be a little flakey - run it a few times
		for(int i = 0; i < 4; i++) {
			tests.add(new StatementMirrorProc("insert into ita (`crappy`) select length(b.crappy) from itb b where b.id > 8"));
			tests.add(new StatementMirrorFun("select * from ita order by id"));
			tests.add(new StatementMirrorProc("delete from ita"));
		}
		// argh - planned correctly but fails at execution time
		// tests.add(new StatementMirrorFun("select a.crappy as crap, length(a.junky) from ita a order by crap"));
	}
	
	private void addParserTests(List<MirrorTest> tests) {
		tests.add(new StatementMirrorProc("create table pe26 (a int)"));
		tests.add(new StatementMirrorProc("insert into pe26 values (0),(1),(NULL)"));
		tests.add(new StatementMirrorFun("select * from pe26 where not(a and 1)"));
	}
	
}
