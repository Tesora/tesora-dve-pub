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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.NativeDatabaseDDL;
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
				"schema","utf8",null);
	private static NativeDDL nativeDDL =
		new NativeDDL(new NativeDatabaseDDL("ctadb","database","utf8",null));
	
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
		"create table broadsrc (id int auto_increment, sid int, whatsup varchar(32) not null, primary key(id)) ",
		"create table randsrc (id int, fid int, price decimal(15,5), primary key(id), foreign key (fid) references broadsrc (id)) ",
		"create table ransrc (id int, fid int not null, primary key(id)) ",
		"create table statsrc (id int, fid int, hula varchar(32), primary key (id)) "
	};
	
	private static final String[] dists = new String[] {
		"/*#dve broadcast distribute */",
		"/*#dve random distribute */",
		"/*#dve range distribute on (id) using oner */",
		"/*#dve static distribute on (id) */"
	};
	
	private static final String[] srctabs = new String[] {
		"broadsrc", "randsrc", "ransrc", "statsrc"
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
				for(int i = 0; i < schema.length; i++)
					mr.getConnection().execute(schema[i] + " " + dists[i]);
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
				tests.add(new ShowCreateTable(targtabs[i]));
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
	public void testA() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			for(int i = 0; i < srctabs.length; i++) {
				tests.add(new StatementMirrorProc(String.format("create table %s as select *, 1 as extra from %s",targtabs[i],srctabs[i])));
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
	
	@Test
	public void testB() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			for(int i = 0; i < srctabs.length; i++) {
				tests.add(new StatementMirrorProc(
						String.format("create table %s (tid int, primary key (id)) default charset=utf8  as select * from %s",
								targtabs[i],srctabs[i])));
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
	
	@Test
	public void testC() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			for(int i = 0; i < srctabs.length; i++) {
				tests.add(new StatementMirrorProc(
						String.format("create table %s (id int, fid int, sid int default '15') as select 1 as fid",
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
	
	@Test
	public void testD() throws Throwable {
		try {
			String infoqkern = 
					"select model_type, model_name, column_name, vector_position "
							+"from information_schema.distributions "
							+"where database_name = '%s' and table_name = '%s'";
			Object[] expected = new Object[] {
					br(nr,"Broadcast",null,null,null),
					br(nr,"Random",null,null,null),
					br(nr,"Range","oner","id",1),
					br(nr,"Static",null,"id",1)
			};
			ConnectionResource conn = getMultiConnection();
			for(int i = 0; i < srctabs.length; i++) {
				int srcrows = (i == 1 ? 9 : 10);
				for(int d = 0; d < dists.length; d++) {
					conn.execute(String.format(
							"create table %s %s as select * from %s",
							targtabs[d],dists[d],srctabs[i]));
					conn.assertResults(String.format(infoqkern,"ctadb",targtabs[d]),
							(Object[])expected[d]);
					conn.assertResults(String.format("select count(*) from %s",targtabs[d]),
							br(nr,(long)srcrows));
					if (d > 1 && srcrows == 10) {
						// range, static
						conn.assertResults(
								String.format("select '%s' as dist, count(*), cast(@dve_sitename as char(10)) as site from %s group by site order by site",
										dists[d],targtabs[d]),
								br(nr,dists[d],2L,"sys0",
								   nr,dists[d],2L,"sys1",
								   nr,dists[d],2L,"sys2",
								   nr,dists[d],2L,"sys3",
								   nr,dists[d],2L,"sys4"));
					}
					conn.execute("drop table " + targtabs[d]);
				}
			}
			
		} finally {
			cleanup(targtabs);
		}
	}
	
	@Test
	public void testE() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			for(int i = 1; i < srctabs.length; i++) {
				
				tests.add(new StatementMirrorProc(
						String.format("create table %s %s as select t.id, b.fid from randsrc b inner join %s t on b.id = t.id",
								targtabs[i],dists[i],srctabs[i])));
				tests.add(new StatementMirrorFun(String.format("select count(*) from %s",targtabs[i])));
			}
			runTest(tests);
		} finally {
			cleanup(targtabs);
		}
	}
	
	@Test
	public void testF() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			String kern = 
					"create table %s (id int, fid int) %s as select whatsup as id, id as fid from broadsrc";
			final String canna = 
				String.format(kern, "canna",dists[2]);
			tests.add(new StatementMirrorProc("set sql_mode = 'NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,STRICT_ALL_TABLES'"));
			tests.add(new MirrorProc() {

				@Override
				public ResourceResponse execute(TestResource mr)
						throws Throwable {
					try {
						mr.getConnection().execute(canna);
						fail("should not be able to create table (bad data) for " + (mr.getDDL().isNative() ? "native" : "pe") + " connection");
					} catch (SQLException sqle) {
						if (sqle.getMessage().indexOf("Incorrect integer value") == -1)
							throw sqle;
					}
					return null;
				}
				
			});
			tests.add(new StatementMirrorFun(false,true,"select table_name from information_schema.tables where table_schema = 'ctadb' and table_name = 'canna'"));
			runTest(tests);
		} finally {
			cleanup("canna");
		}
	}

	@Test
	public void testG() throws Throwable {
		try {
			ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			tests.add(new StatementMirrorProc("set foreign_key_checks=0"));
			// forward ref
			tests.add(new StatementMirrorProc("create table refd (id int auto_increment, fid int, primary key(id), foreign key (fid) references targ (id))"));
			tests.add(new StatementMirrorProc("create table targ (primary key (id)) /*#dve broadcast distribute */ as select * from randsrc"));			
//			tests.add(new ShowCreateTable("refd"));
			tests.add(new ShowCreateTable("targ"));
			tests.add(new StatementMirrorProc("drop table refd"));
			tests.add(new StatementMirrorProc("drop table targ"));
			tests.add(new StatementMirrorProc("create table refd (primary key (id), foreign key (fid) references targ (id)) as select id, fid from randsrc"));
			tests.add(new StatementMirrorProc("create table targ (primary key (id)) /*#dve broadcast distribute */ as select * from broadsrc"));
			tests.add(new ShowCreateTable("targ"));
			tests.add(new StatementMirrorProc("drop table refd"));
			tests.add(new StatementMirrorProc("drop table targ"));
			tests.add(new StatementMirrorProc("create table refd (primary key (id), foreign key (fid) references targ(id)) as select id, fid from randsrc"));
			tests.add(new StatementMirrorProc("create table targ (id int auto_increment, whatevs varchar(32), primary key (id)) /*#dve broadcast distribute */"));
			tests.add(new ShowCreateTable("targ"));
			runTest(tests);
		} finally {
			cleanup("targ","refd");
		}
	}
	
}
