package com.tesora.dve.db.mysql.portal;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaMirrorTest;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.DatabaseDDL;
import com.tesora.dve.sql.util.MapOfMaps;
import com.tesora.dve.sql.util.MirrorFun;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.NativeDatabaseDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PEDatabaseDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class ComplexPreparedStatementTest extends SchemaMirrorTest {

	private static final int SITES = 3;

	private static PEDDL buildPEDDL() {
		PEDDL out = new PEDDL();
		StorageGroupDDL sgddl = new StorageGroupDDL("sys",SITES,"sysg");
		out.withStorageGroup(sgddl)
			.withDatabase(new PEDatabaseDDL("sysdb").withStorageGroup(sgddl))
//			.withDatabase(new PEDatabaseDDL("tsysdb").withStorageGroup(sgddl))
			;
		return out;
	}
	
	private static NativeDDL buildNativeDDL() {
		NativeDDL out = new NativeDDL();
		out.withDatabase(new NativeDatabaseDDL("sysdb"))
//			.withDatabase(new NativeDatabaseDDL("tsysdb"))
			;
		return out;
	}
	
	private static final ProjectDDL sysDDL = buildPEDDL();
	static final NativeDDL nativeDDL = buildNativeDDL();

	private static final PreparedStatementRegistry stmts = new PreparedStatementRegistry();
	private static final Population population = new Population();
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	private static Properties getUrlOptions() {
		Properties props = new Properties();
		props.setProperty("useServerPrepStmts","true");
		props.setProperty("emulateUnsupportedPstmts","false");
		return props;
	}
		
	@Override
	protected ConnectionResource createConnection(ProjectDDL p) throws Throwable {
		if (p == getNativeDDL()) 
			return new DBHelperConnectionResource(getUrlOptions()); 
		else if (p == getMultiDDL()) 
			return new PortalDBHelperConnectionResource(getUrlOptions());

		throw new PEException("Unsupported ProjectDDL type " + p.getClass());
	}
		
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL,null,nativeDDL,getSchema());
	}

	@AfterClass
	public static void teardown() throws Throwable {
		// seems like this test is occasionally too fast on the shutdown
		stmts.clear();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ie) {
			// ignore
		}
	}
	
	static final String[] tabNames = new String[] {
			"LA", "RA",
			"LRa", "RRa",
			"LRb", "RRb",
			"LB", "RB"
	};

	private static final String[] lefts = new String[] { "LA", "LRa", "LRb", "LB" };
	private static final String[] rights = new String[] { "RA", "RRa", "RRb", "RB" };

	private static final String[] testNames = lefts; 
			//new String[] { "LRa" };
	
	private List<MirrorTest> buildSetup() {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		buildPreparePopulate(tests);
		buildPopulate(tests);
		return tests;
	}
	
	@Test
	public void testQuery1() throws Throwable {
		List<MirrorTest> tests = buildSetup();
		for(int i = 0; i < testNames.length; i++) {
			tests.add(new PrepareMirrorProc("q11_" + testNames[i],"select * from " + testNames[i] + " where nusid <= ? order by id"));
			tests.add(new PrepareMirrorProc("q12_" + testNames[i],"select * from " + testNames[i] + " where (nusid % 2) = ? order by id"));
			tests.add(new PrepareMirrorProc("q13_" + testNames[i],"select ?, fid from " + testNames[i] + " where test in (?,?,?,?)"));
			tests.add(new PrepareMirrorProc("q14_" + testNames[i],"select id from " + testNames[i] + " order by id limit ? offset ?"));
			tests.add(new PrepareMirrorProc("q15_" + testNames[i],"select id from " + testNames[i] + " order by id limit 1 offset 2"));
			tests.add(new PrepareMirrorProc("q16_" + testNames[i],"select id from " + testNames[i] + " order by id limit 5 offset 10"));
		}
		List<Object> halfRoughly = Collections.singletonList((Object)new Integer(6));
		List<Object> zero = Collections.singletonList(((Object)new Integer(0)));
		List<Object> one = Collections.singletonList(((Object)new Integer(1)));
		List<Object> projArg = Arrays.asList(new Object[] { "ribbit", "a", "e", "i", "o" });
		List<Object> smallLim = Arrays.asList(new Object[] { new Integer(2), new Integer(1) });
		List<Object> bigLim = Arrays.asList(new Object[] { new Integer(5), new Integer(10) });
		for(int i = 0; i < testNames.length; i++) { 
			tests.add(new ExecuteMirrorFun(false,"q11_" + testNames[i],halfRoughly));
			tests.add(new ExecuteMirrorFun(false,"q12_" + testNames[i],zero));
			tests.add(new ExecuteMirrorFun(false,"q12_" + testNames[i],one));
			tests.add(new ExecuteMirrorFun(true,"q13_" + testNames[i],projArg));
			tests.add(new ExecuteMirrorFun(true,"q14_" + testNames[i],smallLim));
			tests.add(new ExecuteMirrorFun(true,"q14_" + testNames[i],bigLim));
			tests.add(new ExecuteMirrorFun(true,"q15_" + testNames[i],Collections.emptyList()));
			tests.add(new ExecuteMirrorFun(true,"q16_" + testNames[i],Collections.emptyList()));
		}
		runTest(tests);
	}

	@Test
	public void testUpdate1() throws Throwable {
		List<MirrorTest> tests = buildSetup();
		// this test is destructive
		requiresPopulation();
		for(int i = 0; i < tabNames.length; i++) {
			tests.add(new PrepareMirrorProc("q21_" + tabNames[i],"delete from " + tabNames[i] + " where nufid = ?"));
			tests.add(new PrepareMirrorProc("q23_" + tabNames[i],"update " + tabNames[i] + " set test = ?, fid = ?, nusid = ? where id > ?"));
		}
		for(int i = 0; i < lefts.length; i++) {
			tests.add(new PrepareMirrorProc("q22_" + lefts[i],"insert into " + rights[i] + colspec + " select 2 * id as `fid`, ?, 2 * nufid as `nufid`, id / 2 as `nusid` from " + lefts[i] + " where nufid = ? order by id"));
		}
		List<Object> two = Collections.singletonList((Object)new Integer(2));
		for(int i = 0; i < tabNames.length; i++) {
			tests.add(new ExecuteMirrorProc("q21_" + tabNames[i],two));
		}
		List<Object> iid = Arrays.asList(new Object[] { "z", new Integer(1) });
		List<Object> iid2 = Arrays.asList(new Object[] { "y", new Integer(57) });
		for(int i = 0; i < lefts.length; i++) {
			tests.add(new ExecuteMirrorProc("q22_" + lefts[i],iid));
			tests.add(new ExecuteMirrorProc("q22_" + lefts[i],iid2));
		}
		for(int i = 0; i < rights.length; i++) {
			tests.add(new StatementMirrorFun("select * from " + rights[i] + " order by id asc"));
		}
		List<Object> combo1 = Arrays.asList(new Object[] { "x", new Integer(52), new Integer(77) , new Integer(10)});
		List<Object> combo2 = Arrays.asList(new Object[] { "w", new Integer(-202), new Integer(81), new Integer(20) });
		for(int i = 0; i < tabNames.length; i++) {
			tests.add(new ExecuteMirrorProc("q23_" + tabNames[i],(i %2 == 0) ? combo1 : combo2));
		}
		for(int i = 0; i < tabNames.length; i++) {
			tests.add(new StatementMirrorFun("select * from " + tabNames[i] + " order by id asc"));
		}
		runTest(tests);
	}
	
	@Test
	public void testJoin1() throws Throwable {
		List<MirrorTest> tests = buildSetup();
		String sql = "select l.*, r.* from #L l left outer join #R r on l.id=r.nufid where l.test in (?,?)";
		List<String> all = new ArrayList<String>();
		for(int l = 0; l < lefts.length; l++) {
			for(int r = 0; r < rights.length; r++) {
				String actual = sql.replaceFirst("#L", lefts[l]).replaceFirst("#R", rights[r]);
				String key = "q31_" + lefts[l] + "_" + rights[r];
				tests.add(new PrepareMirrorProc(key,actual));
				all.add(key);
			}
		}
		List<Object> values = Arrays.asList(new Object[] { "a", "b" });
		for(String s : all) {
			tests.add(new ExecuteMirrorFun(true,s,values));
		}
		runTest(tests);
	}
	
	@Test
	public void testPE1387() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorProc("create table pe1387 (id int, payload varchar(32), primary key (id)) /*#dve broadcast distribute */"));
		out.add(new StatementMirrorProc("insert into pe1387 (id, payload) values (1,'one'),(2,'two'),(3,'three'),(4,'four')"));
		out.add(new PrepareMirrorProc("pe1387","select id from pe1387 where payload like ? order by id"));
		out.add(new ExecuteMirrorFun(false,"pe1387",Arrays.asList(new Object[] { "%o%" })));
		out.add(new ExecuteMirrorFun(false,"pe1387",Arrays.asList(new Object[] { "t%" })));
		runTest(out);
	}

	@Test
	public void testDVE1498() throws Throwable {
		List<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorProc("create table DVE1498 (id mediumint(9)) /*#dve RANDOM DISTRIBUTE */"));
		out.add(new StatementMirrorProc("insert into DVE1498 values (0)"));
		out.add(new StatementMirrorProc("select distinct * from DVE1498"));
		runTest(out);
	}

	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				boolean ext = !nativeDDL.equals(mr.getDDL());
				DatabaseDDL db = mr.getDDL().getDatabases().get(0);
				// declare the tables
				ResourceResponse rr = null;
				if (ext) { 
					// declare the range
					mr.getConnection().execute("/*#dve create range arange_" + db.getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName() + " */");
					mr.getConnection().execute("/*#dve create range brange_" + db.getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName() + " */");
				}
				String body = "(`id` int auto_increment, `fid` int, `nufid` int, `nusid` int, `test` varchar(32), "
						+ " primary key (`id`), unique key (`fid`), index (`nufid`), index (`nusid`) )";
				String[] dists = new String[] {
						"random distribute",
						"range distribute on (`id`) using arange_" + db.getDatabaseName(),
						"range distribute on (`id`) using brange_" + db.getDatabaseName(),
						"broadcast distribute"
				};
				for(int i = 0; i < dists.length; i++) {
					for(int j = 0; j < 2; j++) {
						String tna = tabNames[2*i + j];
						StringBuffer buf = new StringBuffer();
						buf.append("create table `").append(tna).append("` ").append(body);
						buf.append(" /*#dve ").append(dists[i]).append(" */");
						rr = mr.getConnection().execute(buf.toString());
					}
				}
				
				// create one table in the second database
//				db = mr.getDDL().getDatabases().get(1);
//				mr.getConnection().execute("use " + db.getDatabaseName());
//				StringBuffer buf = new StringBuffer();
//				buf.append("create table `otab` ").append(body);
//				buf.append(" /*#dve ").append(dists[1]).append(" */");
//				mr.getConnection().execute(buf.toString());

				return rr;
			}
		});
		return out;
	}	

	
	private static final String colspec = " (`fid`, `test`, `nufid`, `nusid`)";
	private static final String wideColspec = " (`id`, `fid`, `test`, `nufid`, `nusid`)";
	private static boolean prepared = false;
	
	private static void buildPreparePopulate(List<MirrorTest> tests) {
		if (prepared) return;
		prepared = true;
		for(int i = 0; i < testNames.length; i++) {
			population.prepare(tests, testNames[i]);
		}		
	}
	
	private static boolean populated = false;
	
	private static void buildPopulate(List<MirrorTest> tests) {
		if (populated) return;
		populated = true;
		for(int i = 0; i < testNames.length; i++) {
			population.populate(tests, testNames[i]);
		}		
	}
	
	private static void requiresPopulation() {
		populated= false;
		prepared = false;
	}
		
	private static class PreparedStatementRegistry {
		
		private MapOfMaps<TestResource, String, Object> stmts = new MapOfMaps<TestResource,String,Object>();
		
		public PreparedStatementRegistry() {
		}
		
		public Object put(TestResource tr, String name, String query) throws Throwable {
			Object already = stmts.get(tr,name);
			if (already != null)
				tr.getConnection().destroyPrepared(already);
			Object ps = tr.getConnection().prepare(null, query);
			if (ps == null)
				throw new IllegalStateException("Prepare on '" + query + "' did not yield an identifier");
			stmts.put(tr,name,ps);
			return ps;
		}
		
		public Object get(TestResource tr, String name) {
			return stmts.get(tr,name);
		}
		
		public void clear() {
			stmts.clear();
		}
	}
	
	private static class Population {

		private static final char[] letters = " abcdefghijklmnopqrstuv".toCharArray();

		private List<Object> buildTuple(int i, boolean wide) {
			ArrayList<Object> out = new ArrayList<Object>();
			if (wide)
				out.add(i);
			out.add(2*i);
			StringBuilder buf = new StringBuilder();
			boolean outer = (i % 2 == 0);
			boolean inner = (i % 3 == 0);
			if (outer)
				buf.append("'");
			buf.append(letters[i]);
			if (inner)
				buf.append("'");
			if (outer)
				buf.append("'");
			out.add(buf.toString());
			out.add( i > 10 ? 2 : 1);
			out.add( i / 2);
			return out;
		}
		
		private final List<Object> fiveParams1 = new ArrayList<Object>(); // 1,2,3,4,5
		private final List<Object> fiveParams2 = new ArrayList<Object>(); // 6,7,8,9,10
		private final List<Object> fourParams1 = new ArrayList<Object>(); // 11,12,13,14
		private final List<Object> fourParams2 = new ArrayList<Object>(); // 15,16,17,18
		private final List<Object> oneParams1 = new ArrayList<Object>(); // 19
		private final List<Object> oneParams2 = new ArrayList<Object>(); // 20

		private void initializePopulationParams() {
			for(int i = 1; i < 6; i++)
				fiveParams1.addAll(buildTuple(i,true));
			for(int i = 6; i < 11; i++)
				fiveParams2.addAll(buildTuple(i,true));
			for(int i = 11; i < 15; i++)
				fourParams1.addAll(buildTuple(i,false));
			for(int i = 15; i < 19; i++)
				fourParams2.addAll(buildTuple(i,false));
			oneParams1.addAll(buildTuple(19,false));
			oneParams2.addAll(buildTuple(20,false));
		}

		public Population() {
			initializePopulationParams();
		}
		
		private String buildInsertQuery(String tabName, int ntuples, boolean wide) {
			StringBuilder buf = new StringBuilder();
			buf.append("insert into ").append(tabName).append((wide ? wideColspec : colspec)).append(" values ");
			for(int i = 0; i < ntuples; i++) {
				if (i > 0)
					buf.append(", ");
				if (wide)
					buf.append("(?,?,?,?,?)");
				else
					buf.append("(?,?,?,?)");
			}
			return buf.toString();
		}

		public void prepare(List<MirrorTest> out, String tabName) {
			out.add(new PrepareMirrorProc("pop_delete_" + tabName, 
					//"delete from " + tabName
					"truncate " + tabName
					));
			out.add(new PrepareMirrorProc("pop_insert5_" + tabName, buildInsertQuery(tabName,5,true)));
			out.add(new PrepareMirrorProc("pop_insert4_" + tabName, buildInsertQuery(tabName,4,false)));
			out.add(new PrepareMirrorProc("pop_insert1_" + tabName, buildInsertQuery(tabName,1,false)));
		}
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void populate(List<MirrorTest> tests, final String tabName) {
			tests.add(new ExecuteMirrorProc("pop_delete_" + tabName, Collections.EMPTY_LIST));
			final String[] keys = new String[] { 
					"pop_insert5_" + tabName, "pop_insert4_" + tabName, "pop_insert1_" + tabName 
			};
			final List[] params = new List[] {
					fiveParams1, fiveParams2, fourParams1, fourParams2, oneParams1, oneParams2
			};
			for(int i = 0; i < keys.length; i++) {
				for(int j = 0; j < 2; j++) {
					tests.add(new ExecuteMirrorProc(keys[i], params[2*i + j]));
				}
			}
		}
		
	}
	
	private static class PrepareMirrorProc extends MirrorProc {
		
		private String pstmt;
		private String name;
		
		public PrepareMirrorProc(String n, String q) {
			super();
			pstmt = q;
			name = n;
		}

		@Override
		public ResourceResponse execute(TestResource mr) throws Throwable {
			if (mr == null) return null;
			stmts.put(mr, name, pstmt);
			return null;
		}
		
	}
	
	private static class ExecuteMirrorProc extends MirrorProc {
		
		private String name;
		private List<Object> params;
		
		public ExecuteMirrorProc(String n, List<Object> p) {
			super();
			name = n;
			params = p;
		}

		@Override
		public ResourceResponse execute(TestResource mr) throws Throwable {
			if (mr == null) return null;
			Object ps = stmts.get(mr,name);
			if (ps == null)
				throw new IllegalStateException("No prepared statement for " + name + " on resource " + mr.getDDL().getDatabaseName());
			try {
				return mr.getConnection().executePrepared(ps, params);
			} catch (Throwable t) {
				throw t;
			}
		}
	}
	
	private static class ExecuteMirrorFun extends MirrorFun {
		
		private String name;
		private List<Object> params;
		
		public ExecuteMirrorFun(boolean unordered, String name, List<Object> params) {
			super(unordered,false);
			this.name = name;
			this.params = params;
		}

		@Override
		public ResourceResponse execute(TestResource mr) throws Throwable {
			if (mr == null) return null;
			Object ps = stmts.get(mr,name);
			return mr.getConnection().executePrepared(ps, params);
		}
	}
	
}
