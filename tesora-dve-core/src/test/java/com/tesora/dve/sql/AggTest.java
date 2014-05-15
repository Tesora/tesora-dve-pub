// OS_STATUS: public
package com.tesora.dve.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

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

public class AggTest extends SchemaMirrorTest {

	
	private static final int SITES = 5;

	private static ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	private static ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",1,"checkg"),
				"schema");
	private static NativeDDL nativeDDL =
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
	
	// tabNames is the regular test
	static final String[] tabNames = new String[] { "B", "S", "A", "R" };
	static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`id`)",
		"random distribute",
		"range distribute on (`id`) using "
	};
	private static final String tabBody = 
		" `id` int, `sid` int, `pa` int(10), `pb` int(10), `pc` float, `pd` double, primary key (`id`)";

	// btab is the larger test
	static final String[] bTabNames = new String[] { "BB", "BS", "BA", "BR" };
	static final String[] bDistVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`e`)",
		"random distribute",
		"range distribute on (`e`) using "
	};
	private static final String bTabBody = 
		" `id` int auto_increment, `e` int, `d` int, `c` int, `b` int, `a` int, primary key (id) ";

	
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
				for(int i = 0; i < bTabNames.length; i++) {
					String tn = bTabNames[i];
					StringBuilder buf = new StringBuilder();
					buf.append("create table `").append(tn).append("` ( ").append(bTabBody).append(" ) ");
					if (ext && i < 4) {
						buf.append(bDistVects[i]);
						if ("BR".equals(bTabNames[i]))
							buf.append(" open").append(mr.getDDL().getDatabaseName());
					}
					rr = mr.getConnection().execute(buf.toString());
				}
				return rr;
			}
		});
		int width = 8;
		int first = 0 - width/2;
		ArrayList<String> rows = new ArrayList<String>();
		int rc = 0;
		for(int i = 0; i < width; i++) {
			// id, sid, pa, pb, pc, pd
			rows.add("( " + (++rc) + ", " + i + ", " + (first + i) + "," + (2*i) + ", '" + (first + i) + ".0', '" + (2*i) + ".0' )");
			rows.add("( " + (++rc) + ", " + i + ", " + (width - i) + "," + (2*i + 1) + ", '" + (width - i) + ".0', '" + (2*i + 1) + ".0' )");
		}
		// now we're going to randomize the insert order - we're trying to exploit the pk ordering
		ArrayList<String> reordered = new ArrayList<String>();
		while(!rows.isEmpty()) {
			int ith = Agent.getRandom(rows.size());
			reordered.add(rows.remove(ith));
		}
		String rest = "(`id`, `sid`, `pa`, `pb`, `pc`, `pd`) values " + Functional.join(reordered, ", ");
		for(int i = 0; i < tabNames.length; i++) {
			out.add(new StatementMirrorProc("insert into " + tabNames[i] + rest));
		}
		// also the big agg tables
		ArrayList<String> tuples = new ArrayList<String>();
		int strata = 1;  
		String format = "(%d,%d,%d,%d,%d)";
		for(int a = 1; a <= strata; a++) {
			for(int b = 1; b <= (2*strata); b++) {
				for(int c = 1; c <= (4*strata); c++) {
					for(int d = 1; d <= (8*strata); d++) {
						for(int e = 1; e <= (16*strata); e++) {
							tuples.add(String.format(format,e,d,c,b,a));
						}
					}
				}
			}
		}
		// do the randomization thing too
		// now we're going to randomize the insert order - we're trying to exploit the pk ordering shit
		reordered.clear();
		while(!tuples.isEmpty()) {
			int ith = Agent.getRandom(tuples.size());
			reordered.add(tuples.remove(ith));
		}
		rest = "(`e`,`d`,`c`,`b`,`a`) values " + Functional.join(reordered, ", ");
		for(int i = 0; i < tabNames.length; i++) {
			out.add(new StatementMirrorProc("insert into " + bTabNames[i] + rest));
		}		

		// word, score, count, sid
		Object[] search_test_values = new Object[] {
			new String[] { "s7byu2mc", "26", "0.016390416188169", "1" },
			new String[] { "searchcommenttoggletestcase", "1", "0.17609125905568", "1" },
			new String[] { "new", "1", "0.30102999566398", "1" },
			new String[] { "fm1gox2e", "26", "0.016390416188169", "1" },
			new String[] { "permalink", "11", "0.0377885608894", "1" },
			new String[] { "submitted", "1", "0.30102999566398", "1" },
			new String[] { "x0pirfsd", "11", "0.0377885608894", "1" },
			new String[] { "tue", "1", "0.30102999566398", "1" },
			new String[] { "12202011", "1", "0.30102999566398", "1" },
			new String[] { "1715", "1", "0.30102999566398", "1" },
			new String[] { "el4gchmhd", "1", "0.30102999566398", "1" },
			new String[] { "delete", "11", "0.0377885608894", "1" },
			new String[] { "edit", "11", "0.0377885608894", "1" },
			new String[] { "reply", "11", "0.0377885608894", "1" },
			new String[] { "xl6wpvwz", "26", "0.016390416188169", "2" }
		};
		ArrayList<String> search_total_values = new ArrayList<String>();
		ArrayList<String> search_index_values = new ArrayList<String>();
		for(int i = 0; i < search_test_values.length; i++) {
			String[] row = (String[]) search_test_values[i];
			search_total_values.add("('" + row[0] + "', '" + row[2] + "')");
			search_index_values.add("('" + row[0] + "', '" + row[1] + "', '" + row[3] + "')");
		}
		out.add(new StatementMirrorProc("create table search_total (`word` varchar(50), `count` float, primary key (`word`))"));
		out.add(new StatementMirrorProc("insert into search_total (`word`, `count`) values "
				+ Functional.join(search_total_values, ", ")));
		out.add(new StatementMirrorProc("create table search_index (`word` varchar(50), `sid` int, `score` float, primary key (`word`, `sid`))"));
		out.add(new StatementMirrorProc("insert into search_index (`word`, `score`, `sid`) values "
				+ Functional.join(search_index_values, ", ")));
		return out;
	}
	
	private List<MirrorTest> generate(String format, String[] tables) {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String s : tables)
			out.add(new StatementMirrorFun(true,String.format(format,s)));
		return out;
	}
	
	@SuppressWarnings("unused")
	private void forAll(String format, List<MirrorTest> acc, String[] tables) {
		acc.addAll(generate(format,tables));
	}
	
	private final String[] aggFuns = new String[] { "count", "min", "max", "avg", "sum" };
		
	@Test
	public void hodG() throws Throwable {
		runTest(generate("select count(pb), max(pb) from %s",tabNames));
	}
	
	@Test
	public void hodH() throws Throwable {
		runTest(generate("select count(pb) + max(pb) from %s",tabNames));
	}
	
	@Test
	public void hodI() throws Throwable {
		runTest(generate("select sum(pd), sid from %s group by sid having count(*) > 1", tabNames));
	}

	@Test
	public void testNonGrandAggFunsA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select sid, %s(pa) from %%s group by sid",af),tabNames));
		runTest(out);
	}

	@Test
	public void testDistinctNonGrandAggFunsA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select sid, %s(distinct pa) from %%s group by sid",af),tabNames));
		runTest(out);
	}

	@Test
	public void testDistinctMixedNonGrandAggFunsA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select sid, %s(pa), %s(distinct pa) from %%s group by sid",af,af),tabNames));
		runTest(out);
	}
	
	@Test
	public void testNonGrandAggFunsB() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int i = 0; i < aggFuns.length; i++) 
			out.addAll(generate(String.format("select sid, %s(pa) + %s(pb) from %%s group by sid",aggFuns[i],aggFuns[aggFuns.length - 1 - i]),tabNames));
		runTest(out);
	}

	// come back to this one - requires a multistep plan involving an orderby
	@Ignore
	@Test
	public void testNonGrandAggFunsC() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select sid, %s(pa) + pb from %%s group by sid",af),tabNames));
		runTest(out);
	}
	
	@Test
	public void testNonGrandAggFunsD() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select %s(pb) + 15, sid, pa from %%s group by pa, sid",af), tabNames));
		runTest(out);		
	}
	
	@Test
	public void testNonGrandAggFunsE() throws Throwable {
		// same as D - except we drop a group by
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select %s(pb) + 15, sid, pa from %%s group by sid, pa",af),tabNames));
		runTest(out);		
	}
	
	// grand agg funs
	@Test
	public void testGrandAggFunsA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select %s(pa) from %%s",af),tabNames));
		runTest(out);
	}

	@Test
	public void testDistinctGrandAggFunsA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select %s(distinct pa) from %%s",af),tabNames));
		runTest(out);
	}

	@Test
	public void testDistinctMixedGrandAggFunsA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select %s(pa), %s(distinct pa) from %%s",af,af),tabNames));
		runTest(out);		
	}
	
	@Test
	public void testMultiDistinctGrandAggFunsA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select %s(pa), %s(distinct pb) from %%s",af,af),tabNames));
		runTest(out);				
	}
	
	@Ignore
	@Test
	public void testMixedGrandAggFuns() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String af : aggFuns)
			out.addAll(generate(String.format("select %s(pa) + pb from %%s order by id",af),tabNames));
		runTest(out);
	}
	
	@Test
	public void testGrandAggFunsB() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int i = 1; i < aggFuns.length; i++) 
			out.addAll(generate(String.format("select %s(pb) + %s(pa) from %%s",aggFuns[i],aggFuns[aggFuns.length -1 - i]),tabNames));
		runTest(out);
	}
	
	@Test
	public void testDistinctProjectionA() throws Throwable {
		runTest(generate("select distinct sid from %s",tabNames));
		runTest(generate("select distinct pa from %s",tabNames));
	}
	
	@Test
	public void testDashboard24459() throws Throwable {
		runTest(generate("select count(distinct sid) from %s where pb > 20",tabNames));
	}
	
	@Test
	public void testDrupalExistenceQuery() throws Throwable {
		runTest(generate("select count(*) from (select 1 as expression from %s) subquery",tabNames));
	}

//	private static final String tabBody = 
//		" `id` int, `sid` int, `pa` int(10), `pb` int(10), primary key (`id`)";
	
	@Test
	public void testHavingA() throws Throwable {
		runTest(generate("select max(pa), sid from %s group by sid having avg(pb) > 4",tabNames));
	}
	
	@Test
	public void testHavingB() throws Throwable {
		runTest(generate("select max(pa), sid, avg(pb) hc from %s group by sid having hc > 6",tabNames));
	}
	
	@Test
	public void testHavingC() throws Throwable {
		runTest(generate("select pa, pb, sid from %s group by sid having pa < 0",tabNames));
	}
	
	@Test
	public void testHavingD() throws Throwable {
		runTest(generate("select pa hc, pb, sid from %s group by sid having hc < 0",tabNames));
	}
	
	@Test
	public void testHavingE() throws Throwable {		
		runTest(generate("select p.sid, sum(2.3465869720398 * p.pa * p.pb) from %s p where p.sid > 0 group by p.sid  having count(*) >= 1", tabNames));
	}	
	
	// fails on mysql5.1 - has too much precision on the t.count * i.score column
	// works with 5.5, putting back in circulation
	@Test
	public void testSearchA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorFun(true,"select * from search_total"));
		out.add(new StatementMirrorFun(true,"select * from search_index"));
		out.add(new StatementMirrorFun(true,"select t.word, t.count, i.score, t.count * i.score from search_total t inner join search_index i on t.word = i.word order by t.word"));
		runTest(out);
		
	}
	
	@Test
	public void testSearchB() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorFun(true,"select sum(t.count * i.score) as calc_score from search_total t inner join search_index i on t.word = i.word where i.word = 'searchcommenttoggletestcase' group by i.sid having count(*) >= '1' order by calc_score desc limit 1 offset 0"));
		if (SchemaTest.getDBVersion() == DBVersion.MYSQL_55)
			out.add(new StatementMirrorFun(true,"select sum(t.count * i.score) as calc_score from search_total t inner join search_index i on t.word = i.word group by i.sid order by calc_score"));
		runTest(out);		
	}

	@Test
	public void testTracker37121() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		String[] tabs = tabNames;
		for(int i = 0; i < tabs.length; i++) {
			out.add(new StatementMirrorProc("insert into T select distinct id + 100, sid, pa, pb, pc, pd from " + tabs[i] + " where id = 9"));
			out.add(new StatementMirrorProc("insert into T (id, sid, pa, pb, pc, pd) select distinct id + 101, sid, pa, pb, pc, pd from " + tabs[i] + " where id = 9"));
			out.add(new StatementMirrorFun(true,"select * from T"));
			out.add(new StatementMirrorProc("delete from T where id in (109,110)"));
		}
		runTest(out);				
	}
	
	@Test
	public void testPE92() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorFun(true,"select * from S, A"));
		out.add(new StatementMirrorFun(true,"select count(*) from S, A"));
		runTest(out);
	}
	
	@Test
	public void testEntityApi45355() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorFun(true,"select distinct s.pa, a.pb from S s inner join A a on s.sid = a.sid where a.pd > 5 and s.pb > 5"));
		runTest(out);
		
	}

	@Test
	public void testLocale63968() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorProc("create table locales_source ( `lid` int not null auto_increment, primary key (`lid`) )"));
		out.add(new StatementMirrorProc("create table locales_target ( `lid` int, `translation` blob, `language` varchar(12), primary key(`language`, `lid`))"));
		List<String> values = new ArrayList<String>();
		for(int i = 0; i < 22; i++) values.add("()");
		out.add(new StatementMirrorProc("insert into locales_source values " + Functional.join(values, ", ")));
		out.add(new StatementMirrorProc("insert into locales_target (lid, language, translation) values "
				+ "('9', 'fr', 'un mouton'), "
				+ "('10', 'fr', '@count moutons'), "
				+ "('11', 'fr', 'lundi'), "
				+ "('12', 'fr', 'mardi'), " 
				+ "('13', 'fr', 'mercredi'), "
				+ "('14', 'fr', 'jeudi'), "
				+ "('15', 'fr', 'vendredi'),"
				+ "('16', 'fr', 'samedi'), "
				+ "('17', 'fr', 'dimanche'), "
				+ "('18', 'fr', 'Enregistrer la configuration'), "
				+ "('19', 'fr', 'Enregistrer la configuration'), "
	//			+ "('20', 'fr', 'modifier<img SRC=\"javascript:alert(\'xss\');\">'), "
	//			+ "('21', 'fr', 'supprimer<script>alert(\'xss\');</script>'), "
				+ "('22', 'fr', 'Jour') "));
		runSerialNoMirror(out);
		out.clear();
		out.add(new StatementMirrorFun(true, "select 1 as expression from "
				+ "locales_source s left outer join locales_target t on t.lid = s.lid "
				+ " where (t.translation like '%Montag%') and (language = 'fr')"));
		out.add(new StatementMirrorFun(true, "select count(*) as expression from "
				+ " (select 1 as expression "
				+ "  from locales_source s left outer join locales_target t on t.lid = s.lid "
				+ "  where (t.translation like '%Montag%') and (language = 'fr') ) subquery"));
		runTest(out);
	}
	
	@Test
	public void testPE169() throws Throwable {
		runTest(generate("select count(distinct id) from %s",tabNames));
		
	}
	
	@Test
	public void testPE457() throws Throwable {
		runTest(generate("select count(*) from %s where id in (3,4)",tabNames));
	}

	@Test
	public void testPE797() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		// this one tends to fail randomly - run it a bi tto amke sure
		for(int i = 0; i < 10; i++)
			out.addAll(generate("select a.sid, count(a.sid), count(distinct a.sid) from %s a group by a.sid order by a.sid",tabNames));
		runTest(out);
	}
	
	@Test
	public void testPE394() throws Throwable {
		runTest(generate("select sid, count(sid) as num from %s group by sid having count(sid) >= 2 order by num asc",tabNames));
	}
	
	@Test
	public void testBigA() throws Throwable {
		runTest(generate("select count(distinct e), count(distinct d), count(distinct c), count(distinct b), count(distinct a) from %s",bTabNames));
	}

	@Test
	public void testBigB() throws Throwable {
		runTest(generate("select min(e), max(e), min(d), max(d), min(c), max(c), min(b), max(b), min(a), max(a) from %s", bTabNames));
	}

	@Test
	public void testC() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String s : aggFuns)
			out.addAll(generate(String.format("select %s(e), %s(distinct e) from %%s",s,s),bTabNames));
		runTest(out);
	}
	
	@Test
	public void testD() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String s : aggFuns)
			out.addAll(generate(String.format("select %s(e), %s(distinct e), e from %%s group by e",s,s),bTabNames));
		runTest(out);
	}

	@Test
	public void testE() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String s : aggFuns)
			out.addAll(generate(String.format("select %s(e+d), %s(distinct e+d) from %%s",s,s),bTabNames));
		runTest(out);
	}
	
	@Test
	public void testF() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(String s : aggFuns) 
			out.addAll(generate(String.format("select d, %s(d) from %%s group by d",s),bTabNames));
		runTest(out);
	}
	
	@Test
	public void testG() throws Throwable {
		runTest(generate("select count(e), max(e), min(distinct d+e), avg(distinct c+e) from %s",bTabNames));
	}

}
