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
import org.junit.Test;

import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class UnionTest extends SchemaMirrorTest {

	private static final int SITES = 5;

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	private static final ProjectDDL checkDDL =
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
		setup(sysDDL,checkDDL,nativeDDL,getPopulate());
	}

	static final String[] tabNames = new String[] { "B", "E", "A", "R" }; 
	static final String[] testNames = new String[] { "A", "B" };
	static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`id`)",
		"random distribute",
		"range distribute on (`id`) using "
	};
	
	private static List<MirrorTest> getPopulate() {
		List<MirrorTest> out = new ArrayList<MirrorTest>();
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
				String values = " (name, age, job) values ('John', '25', 'Singer'), ('George', '27', 'Singer'), ('Ringo', '28', 'Drummer'), ('Paul', '26', 'Songwriter'), ('Meredith', '30', 'Speaker')";
				for(String testn : testNames) {
					for(int i = 0; i < tabNames.length; i++) {
						String tableName = testn + tabNames[i];
						StringBuffer buf = new StringBuffer();
						buf.append("create table `").append(tableName).append("` ( ")
							.append("`id` int unsigned not null auto_increment, ")
							.append("`name` varchar(255) not null default '', ")
							.append("`age` int unsigned not null default 0, ")
							.append("`job` varchar(255) not null default 'Undefined', ")
							.append("primary key (`id`), ")
							.append("unique key `name` (`name`), ")
							.append("index `ages` (`age`)) ")
							.append(" engine = InnoDB default character set utf8 ");
						if (ext) {
							buf.append(distVects[i]);
							if ("R".equals(tabNames[i]))
								buf.append(" open").append(mr.getDDL().getDatabaseName());
						}
						rr = mr.getConnection().execute(buf.toString());
						rr = mr.getConnection().execute("insert into " + tableName +  values);
					}
				}
				return rr;
			}
		});
		return out;
	}
	
	private void forAll(String template, String testName, boolean unordered, List<MirrorTest> acc) {
		for(String tn : tabNames) {
			String replName = testName + tn;
			final String actual = template.replace("#", replName);
			acc.add(new StatementMirrorFun(unordered, actual));
		}
	}
	
	@Test
	public void testUnionA() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		forAll("select t.name as name from # t where (age in ('27', '28')) union select t.name as name from # t where (age = '28')","A",true,out);
		runTest(out);
	}
	
	@Test
	public void testUnionB() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		// simpletest718778test
		forAll("select t.name as name from # t where (age in ('27', '28')) union all select t.name as name from # t where (age = '28')","B",true,out);
		runTest(out);
	}
	
	@Test
	public void testUnionC() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		forAll("select t.id, t.name, t.age, t.job from # t where (age in ('27','28')) union select t.id, t.name, t.age, t.job from # t where (age < 30) order by 2","A",false,out);
		runTest(out);
	}
	
	@Test
	public void testUnionD() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		forAll("select t.* from # t where (age in ('27','28')) union select t.* from # t where (age < 30) order by name","A",false,out);
		runTest(out);
	}

	@Test
	public void testUnionE() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		forAll("(select t.id, t.name, t.age, t.job from # t where (age in ('27','28'))) union (select t.id, t.name, t.age, t.job from # t where (age < 30)) order by 2","A",false,out);
		runTest(out);
	}

	@Test
	public void testUnionF() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		forAll("(select t.id, t.name as oc, t.age, t.job from # t where (age in ('27','28'))) union (select t.id, t.name, t.age, t.job from # t where (age < 30)) order by oc","A",false,out);
		runTest(out);
	}

	@Test
	public void testUnionG() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		forAll("(select t.id, t.name, t.age, t.job from # t where (age in ('27','28'))) union (select t.id, t.name, t.age, t.job from # t where (age < 30)) order by name","A",false,out);
		runTest(out);
	}

    @Test
    public void testPE881_unionWithNullsAndStringLiteralAliases() throws Throwable {
        ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
        forAll("(select t.id as tid, NULL as `name`, t.age, t.job from # t where (age in ('27','28'))) union (select t.id, t.name, t.age, NULL as `job` from # t where (age < 30)) order by name,tid","A",false,out);
        runTest(out);
    }
    
    @Test
    public void testUnionH() throws Throwable {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		forAll("(select t.id, t.name, t.age, t.job from # t where (age in ('27','28'))) union (select t.id, t.name, t.age, t.job from # t where (age < 30)) order by name limit 1","A",false,out);
		runTest(out);
    	
    }

}
