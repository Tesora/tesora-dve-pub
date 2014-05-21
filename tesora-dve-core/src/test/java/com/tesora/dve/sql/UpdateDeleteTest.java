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
import java.util.Arrays;
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

public class UpdateDeleteTest extends SchemaMirrorTest {

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
		// return checkDDL;
		return null;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}
	
	// for this test the static setup will create the schema, but then each test will populate the schema as needed
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL,checkDDL,nativeDDL,getSchema());
	}
	
	protected static List<MirrorTest> getSchema() {
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
				for(String tv : sets) {
					for(int i = 0; i < actTabs.size(); i++) {
						String tableName = actTabs.get(i);
						String tn = tv + tableName;
						StringBuilder buf = new StringBuilder();
						buf.append("create table `").append(tn).append("` ( ").append(tabBody).append(" ) ");
						if (ext && i < 4) {
							buf.append(distVects[i]);
							if ("R".equals(tabNames[i]))
								buf.append(" open").append(mr.getDDL().getDatabaseName());
						}
						rr = mr.getConnection().execute(buf.toString());
					}
				}
				return rr;
			}
		});
		return out;
	}

	private static final String[] sets = new String[] { "L", "R" };
	private static final String[] tabNames = new String[] { "B", "S", "A", "R" }; 
	private static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`id`)",
		"random distribute",
		"range distribute on (`id`) using "
	};
	private static final String tabBody = 
		"`id` int, `sid` int, `tid` int, `other` varchar(32), primary key (`id`)";
	
	private List<MirrorTest> getPopulate() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		ArrayList<String> tuples = new ArrayList<String>();
		// we'll want to create joins on {id,sid},{id,tid}, but not exact joins
		// so let's make id multiples of 8, sid multiples of 4, and tid multiples of 2
		// and other will just be the string value of id
		for(int i = 1; i < 8; i++) {
			int a = 4*i;
			int b = 2*i;
			int c = i;
			tuples.add("(" + a + ", " + b + ", " + c + ", '" + a + " " + b + " " + c + "')");
		}
		String values = Functional.join(tuples, ", ");
		for(String tv : sets) {
			for(int i = 0; i < tabNames.length; i++) {
				String tn = tv + tabNames[i];
				out.add(new StatementMirrorProc("delete from " + tn));
				out.add(new StatementMirrorProc("insert into " + tn + " values " + values));
			}
		}
		return out;
	}
	
	private void populate() throws Throwable {
		List<MirrorTest> inserts = getPopulate();
		TestResource[] tr = new TestResource[] { checkResource, sysResource, nativeResource };
		for(TestResource r : tr) {
			for(MirrorTest mt : inserts) {
				mt.execute(r,null);
			}
		}
	}
	
	@Test
	public void testNestedDeleteA() throws Throwable {
		populate();
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int idval = 0; idval < tabNames.length; idval++) {
			for(int tval = 0; tval < tabNames.length; tval++) {
				out.add(new StatementMirrorProc("delete from L" + tabNames[idval] + 
						" where id in (select t.id from R" + tabNames[tval] + " t where t.tid in (" + tval + ", " + (tval + 1) + "))"));
			}
		}
		runTest(out);
	}

	@Test
	public void testNestedDeleteB() throws Throwable {
		populate();
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int idval = 0; idval < tabNames.length; idval++) {
			for(int tval = 0; tval < tabNames.length; tval++) {
				int filter = tval + 1;
				out.add(new StatementMirrorProc("delete from L" + tabNames[idval] + 
						" where id not in (select t.id from R" + tabNames[tval] + " t where t.tid = " + filter + ")")); 
			}
		}
		runTest(out);
	}

	@Test
	public void testNestedUpdateA() throws Throwable {
		populate();
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int idval = 0; idval < tabNames.length; idval++) {
			for(int tval = 0; tval < tabNames.length; tval++) {
				out.add(new StatementMirrorProc("update L" + tabNames[idval] + 
						" set other='update test' where id in (select t.id from R" + tabNames[tval] + 
						" t where t.id in (" + tval + ", " + (tval+1) + "))"));
			}
		}
		runTest(out);
	}

	@Test
	public void testTruncate() throws Throwable {
		populate();
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		for(int i = 0; i < tabNames.length; i++) {
			out.add(new StatementMirrorProc("truncate L" + tabNames[i]));
		}
		runTest(out);
	}
	
}
