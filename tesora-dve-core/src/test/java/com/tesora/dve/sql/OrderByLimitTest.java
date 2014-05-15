// OS_STATUS: public
package com.tesora.dve.sql;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.worker.agent.Agent;

public class OrderByLimitTest extends SchemaMirrorTest {

	private static final int SITES = 5;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	private static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",1,"checkg"),
				"schema");
	private static final NativeDDL nativeDDL =
		new NativeDDL("cdb");
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getSingleDDL() {
		return checkDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL, checkDDL, nativeDDL, getPopulate());
	}

	private static final String[] tabNames = new String[] { "B", "S", "A", "R" }; 
	private static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`id`)",
		"random distribute",
		"range distribute on (`id`) using "
	};
	private static final String tabBody = 
		" `id` int, `sid` int, primary key (`id`)";
	
	private static List<MirrorTest> getPopulate() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				boolean ext = !nativeDDL.equals(mr.getDDL());
				// declare the tables
				ResourceResponse rr = null;
				if (ext) 
					// declare the range
					mr.getConnection().execute("create range open" + mr.getDDL().getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				List<String> actTabs = new ArrayList<String>();
				actTabs.addAll(Arrays.asList(tabNames));
				actTabs.add("T");
				for(int i = 0; i < actTabs.size(); i++) {
					String tn = actTabs.get(i);
					StringBuilder buf = new StringBuilder();
					buf.append("create table `").append(tn).append("` ( ").append(tabBody).append(" ) ");
					if (ext && i < 4) {
						buf.append(distVects[i]);
						if ("R".equals(tabNames[i]))
							buf.append(" open").append(mr.getDDL().getDatabaseName());
					}
					rr = mr.getConnection().execute(buf.toString());
				}
				return rr;
			}
		});
		// we're just going to make some big tables - say 10k rows or so		
		ArrayList<String> rows = new ArrayList<String>();
		for(int i = 1; i <= 1000; i++) {
			rows.add("(" + i + "," + i+ ")");
		}
		// now we're going to randomize the insert order - we're trying to exploit the pk ordering shit
		ArrayList<String> reordered = new ArrayList<String>();
		while(!rows.isEmpty()) {
			int ith = Agent.getRandom(rows.size());
			reordered.add(rows.remove(ith));
		}
		String rest = "(`id`, `sid`) values " + Functional.join(reordered, ", ");
		for(int i = 0; i < tabNames.length; i++) {
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + rest));
		}
		return out;
	}

	private void forAll(String template, boolean ignoreOrder, List<MirrorTest> acc, String[] tables, boolean limitTest) {
		for(int i = 0; i < tables.length; i++) {
			final String actual = template.replace("#", tables[i]);
			acc.add((limitTest ? new LimitMirrorFun(ignoreOrder,actual) : new StatementMirrorFun(ignoreOrder,actual)));
		}
	}

	@Test
	public void testSimpleLimit() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		forAll("select * from # t limit 10",true,tests,tabNames,true);
		runTest(tests);
	}
	
	@Test
	public void testOffsetLimit() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		forAll("select * from # t limit 10,20",true,tests,tabNames,true);
		runTest(tests);
	}

	@Test
	public void testOrderbyLimit() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		forAll("select * from # t order by t.sid limit 10",false,tests,tabNames,false);
		runTest(tests);
	}

	@Test
	public void testOffsetOrderbyLimit() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		forAll("select * from # t order by t.sid limit 10, 20",false,tests,tabNames,false);
		runTest(tests);
	}

	@Test
	public void testPE325() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		forAll("select t.id from # t order by t.sid limit 10, 20",false,tests,tabNames,false);
		runTest(tests);
		
	}
	
	@Test
	public void testPE1526() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("CREATE TABLE IF NOT EXISTS `sttebk` (`ftid` int(10) unsigned NOT NULL DEFAULT 0, `fttype` varchar(32) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve RANDOM DISTRIBUTE */"));
		tests.add(new StatementMirrorProc("TRUNCATE `sttebk`"));
		tests.add(new StatementMirrorProc("INSERT INTO `sttebk` VALUES (1,'bundle1'),(5,'bundle2'),(2,'bundle1'),(3,'bundle1'),(4,'bundle1'),(6,'bundle2');"));
		tests.add(new StatementMirrorFun("SELECT tebk.ftid AS entity_id, tebk.fttype AS bundle, 'tebk' AS entity_type, NULL AS revision_id FROM  sttebk tebk ORDER BY tebk.ftid ASC LIMIT 2 OFFSET 0"));
		tests.add(new StatementMirrorFun("SELECT tebk.ftid AS entity_id, tebk.fttype AS bundle, 'tebk' AS entity_type, NULL AS revision_id FROM  sttebk tebk ORDER BY tebk.ftid ASC LIMIT 6 OFFSET 4"));
		runTest(tests);
	}

	private static class LimitMirrorFun extends StatementMirrorFun {

		public LimitMirrorFun(boolean ordered, String stmt) {
			super(ordered, stmt);
		}
		
		// for the limit tests - with the in memory limit we can no longer rely on ordering
		// or even the same result set - so instead we're going to look at the output counts
		@Override
		public void execute(TestResource checkdb, TestResource sysdb) throws Throwable {
			ResourceResponse cr = execute(checkdb);
			ResourceResponse sr = execute(sysdb);
			if (sysdb == null) return;
			try {
				cr.assertEqualResponse(getContext(), sr);
				int csize = cr.getResults().size();
				int ssize = sr.getResults().size();
				assertEquals("should have same size result set for " + getContext(),csize,ssize);
			} catch (AssertionError ae) {
				// annotate if we actually can get the underlying statement, otherwise don't bother
				if (explainOnFailure)
					throw SchemaTest.annotateFailureWithPlan(ae,getContext(),checkdb,sysdb,null);
				throw ae;
			}
		}

	}
	
}
