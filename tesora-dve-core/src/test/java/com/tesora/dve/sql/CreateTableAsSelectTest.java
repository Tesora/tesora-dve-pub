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

public class CreateTableAsSelectTest extends SchemaMirrorTest {

	private static final int SITES = 5;

	private static ProjectDDL sysDDL =
		new PEDDL("ctadb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	private static NativeDDL nativeDDL =
		new NativeDDL("ctadb");
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL, null, nativeDDL, getPopulate());
	}

	private static final String[] schema = new String[] {
		"create table broadsrc (id int auto_increment, sid int, whatsup varchar(32) not null, primary key(id)) /*#dve broadcast distribute */",
		"create table randsrc (id int, fid int, price decimal(15,5), primary key(id), foreign key (fid) references broadsrc (id)) /*#dve random distribute */",
		"create table ransrc (id int, fid int not null, primary key(id)) /*#dve range distribute on (id) using oner */",
		"create table statsrc (id int, fid int, hula varchar(32), primary key (id)) /*#dve static distribute on (id) */"
	};

	private static final String[] srctabs = new String[] {
		"broadsrc", 
		// "randsrc", "ransrc", "statsrc"
	};
	
	private static final String[] targtabs = new String[] {
		"amherst", "deerfield", "conway", "sunderland"
	};
		
	private static List<MirrorTest> getPopulate() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		// declare a couple of ranges, and some tables (for the src side)
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				if (mr.getDDL().isNative()) return null;
				mr.getConnection().execute("create range oner (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				mr.getConnection().execute("create range twoer (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				return null;
			}
			
		});
		final String[] words = new String[] {
				"creating", "tests", "gets", "old", "after", "a", "while", "but", "someone", "has", "to", "do", "it"
		};
		final double[] numbers = new double[] {
				1.0, 15.3, 17.2, 1000.6, 0.44, 0.01, 0.00, 73.22, 56.1, 123.4567
		};
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				for(String s : schema)
					mr.getConnection().execute(s);
				StringBuilder buf = new StringBuilder();
				buf.append("insert into broadsrc (sid, whatsup) values ");
				boolean first = true;
				for(int i = 0; i < 10; i++) {
					if (first) first = false;
					else buf.append(",");
					if (i % 2 == 0)
						buf.append("(null,");
					else
						buf.append("(").append(i).append(",");
					buf.append("'").append(words[i]).append("')");
				}
				mr.getConnection().execute(buf.toString());
				buf = new StringBuilder();
				buf.append("insert into randsrc (id, fid, price) values ");
				first = true;
				for(int i = 1; i < 10; i++) {
					if (first) first = false;
					else buf.append(",");
					buf.append("(").append(i).append(",").append(i).append(",").append(numbers[i]).append(")");
				}
				mr.getConnection().execute(buf.toString());
				buf = new StringBuilder();
				first = true;
				buf.append("insert into ransrc (id,fid) values ");
				for(int i = 0; i < 10; i++) {
					if (first) first = false;
					else buf.append(",");
					buf.append("(").append(i).append(",").append(2*i).append(")");
				}
				mr.getConnection().execute(buf.toString());
				buf = new StringBuilder();
				first = true;
				buf.append("insert into statsrc (id, fid, hula) values ");
				for(int i = 0; i < 10; i++) {
					if (first) first = false;
					else buf.append(",");
					buf.append("(").append(i).append(",").append(10*i).append(",'").append(words[words.length - i - 1]).append("')");
				}
				mr.getConnection().execute(buf.toString());
				return null;
			}
		});
		return out;
	}

	private void cleanup(String ...tabNames) throws Throwable {
		ArrayList<MirrorTest> drops = new ArrayList<MirrorTest>();
		for(String tn : tabNames)
			drops.add(new StatementMirrorProc("drop table if exists " + tn));
		runTest(drops);
	}
	
	// simplest test possible, just recreate the columns
	@Test
	public void simpleTest() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			for(int i = 0; i < srctabs.length; i++) {
				tests.add(new StatementMirrorProc(String.format("create table %s as select * from %s",targtabs[i],srctabs[i])));
				tests.add(new StatementMirrorFun(String.format("select * from %s order by id",targtabs[i])));
				tests.add(new StatementMirrorFun(false,true,"select table_name from information_schema.tables where table_schema = 'ctadb'"));
				tests.add(new StatementMirrorFun(false,true,String.format("select column_name from information_schema.columns where table_name = '%s' and table_schema = 'ctadb'",targtabs[i])));
			}
			runTest(tests);
		} finally {
			cleanup(targtabs);
		}
	}
	
	@Test
	public void testImplicitA() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			for(int i = 0; i < srctabs.length; i++) {
				tests.add(new StatementMirrorProc(String.format("create table %s as select *, 1 as extra from %s",targtabs[i],srctabs[i])));
				tests.add(new StatementMirrorFun(String.format("select * from %s order by id",targtabs[i])));
				tests.add(new StatementMirrorFun(false,true,String.format(
						"select column_name from information_schema.columns where table_name = '%s' and table_schema = 'ctadb'",
						targtabs[i])));
			}
			runTest(tests);
		} finally {
			cleanup(targtabs);
		}
	}
	
	@Test
	public void testMixedA() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			for(int i = 0; i < srctabs.length; i++) {
				tests.add(new StatementMirrorProc(
						String.format("create table %s (tid int, primary key (id)) as select * from %s",
								targtabs[i],srctabs[i])));
				tests.add(new StatementMirrorFun(String.format("select * from %s order by id",targtabs[i])));
				tests.add(new StatementMirrorFun(false,true,String.format(
						"select column_name from information_schema.columns where table_name = '%s' and table_schema = 'ctadb'",
						targtabs[i])));						
			}
			runTest(tests);
		} finally {
			cleanup(targtabs);
		}
	}
	
	@Test
	public void testExplicitOnly() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			for(int i = 0; i < srctabs.length; i++) {
				tests.add(new StatementMirrorProc(
						String.format("create table %s (id int, fid int, sid int) default charset=utf8 as select 1 as fid",
								targtabs[i], srctabs[i])));
				tests.add(new ShowCreateTable(targtabs[i]));
				tests.add(new StatementMirrorFun(String.format("select * from %s order by id",targtabs[i])));
				tests.add(new StatementMirrorFun(false,true,String.format(
						"select column_name from information_schema.columns where table_name = '%s' and table_schema = 'ctadb'",
						targtabs[i])));						
			}
			runTest(tests);
		} finally {
			cleanup(targtabs);
		}
	}
	
}
