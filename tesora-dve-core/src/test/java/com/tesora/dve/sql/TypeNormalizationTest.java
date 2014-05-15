// OS_STATUS: public
package com.tesora.dve.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.NativeDatabaseDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PEDatabaseDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class TypeNormalizationTest extends ProxySchemaMirrorTest {

	private static PEDDL buildPEDDL() {
		PEDDL out = new PEDDL();
		StorageGroupDDL sgddl = new StorageGroupDDL("sys",1,"sysg");
		out.withStorageGroup(sgddl)
			.withDatabase(new PEDatabaseDDL("pedb").withStorageGroup(sgddl));
		return out;
	}
	
	private static NativeDDL buildNativeDDL() {
		NativeDDL out = new NativeDDL();
		out.withDatabase(new NativeDatabaseDDL("sysdb"));
		return out;
	}
	
	private static final ProjectDDL sysDDL = buildPEDDL();
	private static final NativeDDL nativeDDL = buildNativeDDL();
	
	@Override
	protected ProjectDDL getSingleDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL,null,nativeDDL,Collections.EMPTY_LIST);
	}

	// the purpose of this test is to verify that we normalize the same way - and we verify via the
	// show create table stmt.  however, we need to omit the distribution information - so we create a special
	// mirror fun for getting the create table stmt that edits the resource response for the pe side to remove
	// the distribution information
	private static class ShowCreateTable extends StatementMirrorFun {

		public ShowCreateTable(String tabName) {
			super("show create table " + tabName);
		}
		
		@Override
		public ResourceResponse execute(TestResource mr) throws Throwable {
			if (mr == null) return null;
			ResourceResponse rr = mr.getConnection().fetch(command);
			if (!mr.getDDL().isNative()) {
				// note: we're relying on the fact that getRows returns the actual rows
				ResultRow firstRow = rr.getResults().get(0);
				// second field
				ResultColumn rc = firstRow.getResultColumn(2);
				String cts = (String) rc.getColumnValue();
				int marker = cts.indexOf("/*#dve");
				if (marker > -1) {
					int endMarker = cts.indexOf("*/", marker);
					StringBuilder nc = new StringBuilder();
					nc.append(cts.substring(0, marker - 1));
					nc.append(cts.substring(endMarker + 2));
					rc.setColumnValue(nc.toString());
				}
			}
			return rr;
		}

	}
	
	@Test
	public void testIntegralNormalization() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String[] integTypes = new String[] { "tinyint", "mediumint", "int", "bigint"};
		String[] signedness = new String[] { "", "unsigned", "zerofill", "unsigned zerofill"};
		String[] nullable = new String[] { "", "not null", "null" };
		StringBuilder cts = new StringBuilder();
		cts.append("create table integ_norm (`id` int, ");
		int counter = 1;
		for(String t : integTypes) {
			for(String s : signedness) {
				for(String n : nullable) {
					cts.append("`i").append(counter++).append("` ").append(t).append(" ").append(s).append(" ").append(n).append(",");
				}
			}
		}
		cts.append("key (id)) engine=InnoDB charset utf8");
		tests.add(new StatementMirrorProc(cts.toString()));
		tests.add(new ShowCreateTable("integ_norm"));
		runTest(tests);
	}
	
	@Test
	public void testFloatingPointNormalization() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String[] floatTypes = new String[] { "numeric", "decimal" };
		String[] precs = new String[] { "", "(5)" };
		String[] signedness = new String[] { "", "unsigned" };
		String[] nullable = new String[] { "", "not null", "null" };
		StringBuilder cts = new StringBuilder();
		cts.append("create table fp_norm (`id` int, ");
		int counter = 1;
		for(String t : floatTypes) {
			for(String s : signedness) {
				for(String p : precs) {
					for(String n : nullable) {
						cts.append("`f").append(counter++).append("` ").append(t).append(p).append(" ").append(s).append(" ").append(n).append(",");
					}
				}
			}
		}
		cts.append("key (id)) engine=InnoDB charset utf8");
		tests.add(new StatementMirrorProc(cts.toString()));
		tests.add(new ShowCreateTable("fp_norm"));
		runTest(tests);
		
	}
	
	@Test
	public void testBitTypes() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String[] types = new String[] { "bit" };
		String[] sizes = new String[] { "", "(4)" };
		String[] signedness = new String[] { ""/*, "unsigned"*/ };
		String[] nullable = new String[] { "", "not null", "null" };
		StringBuilder cts = new StringBuilder();		
		cts.append("create table b_norm(`id` int, ");
		int counter = 1;
		for(String t : types) {
			for(String l : sizes) {
				for(String s : signedness) {
					for(String n : nullable) {
						cts.append("`b").append(counter++).append("` ").append(t).append(l).append(" ").append(s).append(" ").append(n).append(",");
					}
				}
			}
		}
		cts.append("key (id)) engine=InnoDB charset utf8");
		tests.add(new StatementMirrorProc(cts.toString()));
		tests.add(new ShowCreateTable("b_norm"));
		runTest(tests);
	}
	
	@Test
	public void testDateTimeNormalization() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String[] nullable = new String[] { "", "not null", "null" };
		String[] types = new String[] { "year", "year(2)", "year(4)", "time", "date", "timestamp", "datetime" };
		StringBuilder cts = new StringBuilder();
		cts.append("create table t_norm(`id` int, ");
		int counter = 1;
		for(String t : types) {
			for(String n : nullable) {
				cts.append("`t").append(counter++).append("` ").append(t).append(" ").append(n).append(",");
			}
		}
		cts.append("key (id)) engine=InnoDB charset utf8");
		tests.add(new StatementMirrorProc(cts.toString()));
		tests.add(new ShowCreateTable("t_norm"));
		runTest(tests);
	}
	
	@Test
	public void testRegularStringNormalization() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String[] types = new String[] { "char", "char(5)", "varchar(22)" };
		String[] bins = new String[] { "", "binary"};
		String[] nullable = new String[] { "", "not null", "null" };
		StringBuilder cts = new StringBuilder();
		cts.append("create table s_norm(`id` int, ");
		int counter = 1;
		for(String t : types) {
			for(String b : bins) {
				for(String n : nullable) {
					cts.append("`s").append(counter++).append("` ").append(t).append(" ").append(b).append(" ").append(n).append(",");
				}
			}
		}
		cts.append("key (id)) engine=InnoDB charset utf8");
		tests.add(new StatementMirrorProc(cts.toString()));
		tests.add(new ShowCreateTable("s_norm"));
		runTest(tests);
	}
	
	@Test
	public void testBinaryStringNormalization() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String[] types = new String[] { "binary", "binary(17)", "varbinary(7)" };
		String[] nullable = new String[] { "", "not null", "null" };
		StringBuilder cts = new StringBuilder();
		cts.append("create table bs_norm(`id` int, ");
		int counter = 1;
		for(String t : types) {
			for(String n : nullable) {
				cts.append("`s").append(counter++).append("` ").append(t).append(" ").append(n).append(",");
			}
		}
		cts.append("key (id)) engine=InnoDB charset utf8");
		tests.add(new StatementMirrorProc(cts.toString()));
		tests.add(new ShowCreateTable("bs_norm"));
		runTest(tests);
	}
	
	@Test
	public void testLOBNormalization() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String[] types = new String[] { "tinyblob", "blob", "mediumblob", "longblob",
				"tinytext", "text", "mediumtext", "longtext" };
		String[] bins = new String[] { "", "binary"};
		String[] nullable = new String[] { "", "not null", "null" };
		StringBuilder cts = new StringBuilder();
		cts.append("create table lob_norm(`id` int, ");
		int counter = 1;
		for(String t : types) {
			String[] bvs = null;
			if (t.indexOf("text") > -1)
				bvs = bins;
			else
				bvs = new String[] { "" };
			for(String b : bvs) {
				for(String n : nullable) {
					cts.append("`s").append(counter++).append("` ").append(t).append(" ").append(b).append(" ").append(n).append(",");
				}
			}
		}
		cts.append("key (id)) engine=InnoDB charset utf8");
		tests.add(new StatementMirrorProc(cts.toString()));
		tests.add(new ShowCreateTable("lob_norm"));
		runTest(tests);
	}
	
	@Test
	public void testEnumSetNormalization() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String[] types = new String[] { "enum('a','b','c')", "enum('a')", "set('a','b')", "set('c')" };
		String[] nullable = new String[] { "", "not null", "null" };
		StringBuilder cts = new StringBuilder();
		cts.append("create table e_norm(`id` int, ");
		int counter = 1;
		for(String t : types) {
			for(String n : nullable) {
				cts.append("`e").append(counter++).append("` ").append(t).append(" ").append(n).append(",");
			}
		}
		cts.append("key (id)) engine=InnoDB charset utf8");
		tests.add(new StatementMirrorProc(cts.toString()));
		tests.add(new ShowCreateTable("e_norm"));
		runTest(tests);
	}
	
	@Test
	public void testSerialNormalization() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		String cts = "create table se_norm(`id` serial) engine=InnoDB charset utf8";
		tests.add(new StatementMirrorProc(cts));
		tests.add(new ShowCreateTable("se_norm"));
		runTest(tests);
	}
	
}
