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
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class AggBugsTest extends SchemaMirrorTest {
	private static final int SITES = 5;

	static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",1,"checkg"),
				"schema");
	static final NativeDDL nativeDDL =
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

	private static final TestRange pe287Range = new TestRange("pe287range","varchar(256), varchar(256)");
	private static final TestTable pe287 = new TestTable("pe287",
			"fname varchar(256), caller varchar(256), callee varchar(256), "
					+ "ct int, wt int, cputime int, mu int, pmu int",
			"fname, callee", pe287Range)
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'main()', 'getcwd', 1, 9, 0, 604, 604")
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'main()', 'define', 1, 28, 0, 604, 604")
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'main()', 'load::includes/bootstrap.inc', 1, 249, 0, 64076, 64076")
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'run_init::includes/bootstrap.inc', 'define', 37, 39, 0, 820, 820 ")
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'main()', 'run_init::includes/bootstrap.inc', 1, 155, 0, 4580, 4420 ")
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'bootstrap', 'array_shift', 6, 7, 0, 468, 468 ")
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'bootstrap_configuration', 'set_error_handler', 1, 5, 0, 672, 672")
			.withTuple(
					"'506c7fbc8a9e9.nonpar.xhprof', 'bootstrap_configuration', 'set_exception_handler', 1, 2, 0, 628, 628 ")
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'environment_initialize', 'strtolower', 1, 2, 0, 664, 664 ")
			.withTuple("'506c7fbc8a9e9.nonpar.xhprof', 'valid_http_host', 'preg_match', 1, 14, 0, 632, 632");
	private static final TestRange pe773Range = new TestRange("pe773range","varchar(10)");
	private static final TestTable pe773 = new TestTable("pe773", "c1 varchar(10), c2 varchar(10)", "c1", pe773Range)
			.withTuple("'001', 'Same Value'")
			.withTuple("'002', 'Same Value'");
				
	private static final TestRange pe865Range = new TestRange("pe865range","int");
	private static final TestTable pe865 = new TestTable("pe865",
			"id int auto_increment, n int, s varchar(32), fp int, primary key(id)",
			"id",pe865Range)
			.withInsertString("(n,s,fp)")
            .withTuple("1,'one',1").withTuple("2,'two',2").withTuple("3,'three',3").withTuple("4,'four',2")
            .withTuple("5,'five',5").withTuple("6,'six',2").withTuple("7,'seven',7").withTuple("8,'eight',2")
            .withTuple("9,'nine',3").withTuple("10,'ten',2").withTuple("11,'eleven',11").withTuple("12,'twelve',2")
            .withTuple("13,'thirteen',13").withTuple("14,'fourteen',2").withTuple("15,'fifteen',3")
            .withTuple("16,'sixteen',2").withTuple("17,'seventeen',17").withTuple("18,'eighteen',2")
            .withTuple("19,'nineteen',19").withTuple("20,'twenty',2").withTuple("21,'twentyone',3")
            .withTuple("22,'twentytwo',2").withTuple("23,'twentythree',23").withTuple("24,'twentyfour',2")
            .withTuple("25,'twentyfive',5");

	
	private static final TestRange pe1370Range = new TestRange("pe1370range","int");
	private static final TestTable pe1370 = new TestTable("pe1370", 
			"col1 int, col2 tinyint unsigned", "col1", pe1370Range)
			.withInsertString("(col1, col2)")
			.withTuple("1,255")
			.withTuple("2,255")
			.withTuple("3,255");

	static TestRange[] ranges = new TestRange[] {
		pe287Range,
		pe773Range,
        pe865Range,
        pe1370Range
	};
	
	private static final TestTable[] tables = new TestTable[] {
		pe287,
		pe773,
        pe865,
        pe1370
	};
		
	private static List<MirrorTest> getPopulate() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				boolean ext = !nativeDDL.equals(mr.getDDL());
				String dbname = mr.getDDL().getDatabases().get(0).getDatabaseName();
				// declare the tables
				ResourceResponse rr = null;
				if (ext) { 
					// declare the ranges
					for(TestRange tr : ranges) {
						rr = mr.getConnection().execute(tr.getDeclaration(mr.getDDL().getPersistentGroup().getName(), dbname));
					}
				}
				// declare the tables
				for(TestTable tt : tables) {
					String[] decls = tt.getTableDecls(ext, dbname);
					for(String d : decls)
						rr = mr.getConnection().execute(d);
				}
				return rr;
			}
		});
		for(TestTable tt : tables) {
			for(String tabName : tt.getTableNames()) {
				out.add(new StatementMirrorProc("insert into " + tabName + " " + tt.getInsertBody()));
			}
		}
		return out;
	}

	@Ignore
	@Test
	public void testPE287() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String tn : pe287.getTableNames()) {
			tests.add(new StatementMirrorFun(true, "select fname, caller, max(wt/ct) from " + tn + " where caller = 'main()'"));
		}
		runTest(tests);
	}
	
	@Ignore
	@Test
	public void testPE773() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String tableName : pe773.getTableNames()) {
			tests.add(new StatementMirrorFun(true, "select c1, count(distinct c2) from " + tableName + " group by c2"));
		}
		runTest(tests);
	}
	
	@Ignore
    @Test
    public void testPE865() throws Throwable {
    	ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
    	String[] tabNames = pe865.getTableNames();
    	String[] testNames = tabNames;
    	for(String tableName : testNames) {
    		tests.add(new StatementMirrorFun(true, true, "select fp, group_concat(s SEPARATOR ' ') from " + tableName + " group by fp order by fp"));
    		tests.add(new StatementMirrorFun(true, true, "select fp, group_concat(n), avg(n) from " + tableName + " group by fp order by fp"));
    		tests.add(new StatementMirrorFun(true, true, "select group_concat(s SEPARATOR ' ') from " + tableName));
    		tests.add(new StatementMirrorFun(true, "select s from " + tableName));

    	}
    	runTest(tests);
    }

	@Test
	public void testPE1370() throws Throwable {
    	ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
    	String[] tblNames = pe1370.getTableNames();
    	for(String tableName : tblNames) {
    		tests.add(new StatementMirrorFun(true, true, "select col1, col2 from " + tableName + " order by col1"));
    	}
    	runTest(tests);
	}
	
	public static final String[] distsuffix = new String [] { "B", "S", "R", "A" };
	public static final String[] distnames = new String[] { "broadcast", "static", "range", "random" };
	
	// we need more infrastructure to handle these test cases.  generally we want to test some agg fun on some data where the data
	// is distributed each of the 4 ways.
	public static class TestTable {
		
		protected String kern;
		protected String distColumns;
		protected String body;
		protected TestRange range;
		protected String insertHeader;
		protected List<String> insertTuples = new ArrayList<String>();
		
		public TestTable(String name, String body, String distOn, TestRange theRange) {
			kern = name;
			distColumns = distOn;
			this.body = body;
			range = theRange;
		}
		
		public TestTable withInsertString(String s) {
			insertHeader = s;
			return this;
		}
		
		public TestTable withTuple(String t) {
			insertTuples.add(t);
			return this;
		}
		
		public String getInsertBody() {
			StringBuffer buf = new StringBuffer();
			if (insertHeader != null) buf.append(insertHeader);
			buf.append("values ");
			for(int i = 0; i < insertTuples.size(); i++) {
				if (i > 0) buf.append(", ");
				buf.append("(").append(insertTuples.get(i)).append(")");
			}
			return buf.toString();
		}
		
		public String[] getTableNames() {
			String[] out = new String[distsuffix.length];
			for(int i = 0; i < distsuffix.length; i++) 
				out[i] = kern + distsuffix[i];
			return out;
		}
		
		public String[] getTableDecls(boolean ext, String rangePrefix) {
			String[] out = new String[distsuffix.length];
			for(int i = 0; i < distsuffix.length; i++) {
				StringBuilder buf = new StringBuilder();
				buf.append("create table `").append(kern).append(distsuffix[i]).append("` (")
					.append(body).append(")");
				if (ext) {
					buf.append(" ").append(distnames[i]).append(" distribute");
					if ("S".equals(distsuffix[i]) || "R".equals(distsuffix[i])) {
						buf.append(" on (").append(distColumns).append(")");
						if ("R".equals(distsuffix[i])) {
							buf.append(" using ").append(range.getName(rangePrefix));
						}
					}
				}
				out[i] = buf.toString();
			}
			return out;
		}
	}

	public static class TestRange {
		
		protected String name;
		protected String signature;
		
		public TestRange(String name, String sig) {
			this.name = name;
			this.signature = sig;
		}
		
		public String getDeclaration(String sgname, String prefix) {
			return "create range " + prefix + name + " (" + signature + ") persistent group " + sgname;
		}
		
		public String getName(String prefix) {
			return prefix + name;
		}
	}
}
