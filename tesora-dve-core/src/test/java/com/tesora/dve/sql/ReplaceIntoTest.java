// OS_STATUS: public
package com.tesora.dve.sql;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
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

public class ReplaceIntoTest extends SchemaMirrorTest {

	private static final int SITES = 5;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	private static final NativeDDL nativeDDL =
		new NativeDDL("cdb");
	
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL, null, nativeDDL, getPopulate());
	}

	private static final String[] tabNames = new String[] { 
		"B", 
		"S", 
		"A", 
		"R" 
		}; 
	private static final String[] tabNamesa = new String[] {
		"Ba",
		"Sa",
		"Aa",
		"Ra"
	};
	private static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`id`)",
		"random distribute",
		"range distribute on (`id`) using open_range"
	};
	
	// so we need a partially populated table as well as a src table for the replace into select version
	// the target tables need to have more than one unique key - so we'll do a pk and a unique key
	// the second unique key should have multiple columns.
	
	private static final String tbody = 
			" (id int, fid int, sid int, what varchar(32), primary key(id), unique key (fid, sid)) ";
	private static final String abody =
			" (id int auto_increment, fid int, sid int, what varchar(32), primary key(id), unique key(fid,sid)) ";
	
	private static final String sbody =
			" (id int, fid int, sid int, what varchar(32)) /*#dve random distribute */";
	
	private static List<MirrorTest> getPopulate() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				if (!mr.getDDL().isNative()) {
					mr.getConnection().execute("create range open_range (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				}
				for(int i = 0; i < tabNames.length; i++) {
					mr.getConnection().execute("create table " + tabNames[i] + tbody + " /*#dve " + distVects[i] + " */");
				}
				ResourceResponse rr = mr.getConnection().execute("create table src" + sbody);
				mr.getConnection().execute("insert into src values " + buildValues(1,100)); 
				for(int i = 0; i < tabNamesa.length; i++) {
					mr.getConnection().execute("create table " + tabNamesa[i] + abody + " /*#dve " + distVects[i] + " */");
				}
				return rr;
			}
			
		});
		return out;
	}

	private static String buildValues(int startAt, int endAt) {
		List<String> tuples = new ArrayList<String>();
		for(int i = startAt; i <= endAt; i++) {
			tuples.add("(" + i + "," + i + "," + i + ",'" + i + "')");			
		}
		return Functional.join(tuples, ", ");
	}
	
	private static String buildValuesa(int startAt, int endAt) {
		List<String> tuples = new ArrayList<String>();
		for(int i = startAt; i <= endAt; i++) {
			tuples.add("(" + i + "," + i + ",'" + i + "')");
		}
		return Functional.join(tuples, ", ");
	}
	
	private void fill(List<MirrorTest> into, String template, String[] tabs) {
		for(String n : tabs) {
			String actual = template.replaceAll("#", n);
			into.add(new StatementMirrorProc(actual));
		}
	}
	
	@Test
	public void testEmptyInsertValues() throws Throwable {
		// in this case the tables are empty, we're just going to insert some values
		// build values first
		String sql = "replace into # values " + buildValues(1,10);
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		fill(tests, "delete from #", tabNames);
		fill(tests, sql, tabNames);
		fill(tests, "select * from # order by id", tabNames);
		fill(tests, "delete from #", tabNames);
		runTest(tests);
	}
	
	@Test
	public void testEmptyInsertSelect() throws Throwable {
		String sql = "replace into # select * from src";
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		fill(tests, "delete from #", tabNames);
		fill(tests, sql, tabNames);
		fill(tests, "select * from # order by id", tabNames);
		fill(tests, "delete from #",tabNames);
		runTest(tests);
	}
	
	@Test
	public void testInsertValues() throws Throwable {
		// first populate a little bit
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		fill(tests, "delete from #", tabNames);
		fill(tests, "insert into # values " + buildValues(1,10),tabNames);
		// so we have (1,1,1,'1') ... (10,10,10,'10') in the tables
		List<String> tuples = new ArrayList<String>();
		// conflicts with the pk, but not the uk
		tuples.add("(1,20,20,'i1')");
		tuples.add("(2,30,30,'i2')");
		// conflicts with the uk, but not the pk
		tuples.add("(11,1,1,'ii1')");
		tuples.add("(12,2,2,'ii2')");
		// conflicts with all three
		tuples.add("(3,3,3,'iii3')");
		tuples.add("(4,4,4,'iii4')");
		// completely new
		tuples.add("(100,100,100,'i100')");
		fill(tests, "replace into # values " + Functional.join(tuples, ","), tabNames);
		fill(tests, "select * from # order by id", tabNames);
		fill(tests, "delete from #",tabNames);
		runTest(tests);
	}
	
	@Test
	public void testSetValues() throws Throwable {
		// essentially the same thing as the previous one, except with single tuples using the set form
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		fill(tests, "delete from #", tabNames);
		fill(tests, "insert into # values " + buildValues(1,10),tabNames);
		fill(tests, "replace into # set id=1, fid=20, sid=20, what='i1'",tabNames);
		fill(tests, "replace into # set id=12, fid=2, sid=2, what='ii2'",tabNames);
		fill(tests, "replace into # set id=3, fid=3, sid=3, what='iii3'",tabNames);
		fill(tests, "replace into # set id=100, fid=100, sid=100, what='i100'",tabNames);
		fill(tests, "select * from # order by id",tabNames);
		fill(tests, "delete from #",tabNames);
		runTest(tests);
	}
	
	@Test
	public void testSelectValues() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		fill(tests, "delete from #", tabNames);
		fill(tests, "insert into # values " + buildValues(1,10),tabNames);
		fill(tests, "replace into # select s.id as id, 20 as fid, 20 as sid, 'i1' as what from src s where s.id = 1",tabNames);
		fill(tests, "replace into # select 12 as id, s.fid, s.sid, 'ii2' as what from src s where s.fid = 2",tabNames);
		fill(tests, "replace into # select s.id as id, s.fid as fid, s.sid as sid, 'iii3' as what from src s where s.id = 3",tabNames);
		fill(tests, "select * from # order by id", tabNames);
		fill(tests, "delete from #",tabNames);
		runTest(tests);		
	}

	@Test
	public void testSelectValuesA() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		fill(tests, "delete from #", tabNamesa);
		fill(tests, "insert into # (fid, sid, what) values " + buildValuesa(1,10), tabNamesa);
		fill(tests, "replace into # (fid, sid, what) select 20 as fid, 20 as sid, 'i1' as what from src s where s.id = 1", tabNamesa);
		fill(tests, "replace into # (fid, sid, what) select s.fid, s.sid, 'ii2' as what from src s where s.fid = 2",tabNamesa);
		fill(tests, "replace into # (fid, sid, what) select s.fid as fid, s.sid as sid, 'iii3' as what from src s where s.id = 3", tabNamesa);
		fill(tests, "select * from # order by id", tabNamesa);
		fill(tests, "delete from #",tabNamesa);
		runTest(tests);
	}
	
}
