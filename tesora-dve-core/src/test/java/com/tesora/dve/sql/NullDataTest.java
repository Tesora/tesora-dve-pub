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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class NullDataTest extends SchemaTest {

	// need one table of every type where one of the dist keys can be nullable
	// need this on more than one site
	// also, expand the range (but don't test static after expanding group)
	
	private static final ProjectDDL testDDL =
		new PEDDL("ndtdb", 
				new StorageGroupDDL("pndt",5,2,"pg"),
				"database");
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(testDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	@Before
	public void before() throws Throwable {
		conn = new ProxyConnectionResource();
		testDDL.create(conn);
	}
	
	@After
	public void after() throws Throwable {
		if(conn != null)
			conn.disconnect();
	}
	
	ProxyConnectionResource conn = null;
	
	private static final String NULL_VALUE = "NULL";
	
	@Test
	public void test() throws Throwable {
		
		// add two ranges
		conn.execute("create range unrange (int) persistent group pg");
		conn.execute("create range birange (int,int) persistent group pg");

		String[] fids = new String[] { "0", "-1", "1", NULL_VALUE, "2000", NULL_VALUE };
		String[] sids = new String[] { "-1", "1", NULL_VALUE, "2000", "0", NULL_VALUE };

		TestTable[] tabs = new TestTable[] {
				new TestTable("tstat",2,StaticDistributionModel.MODEL_NAME,null),
				new TestTable("ostat",1,StaticDistributionModel.MODEL_NAME,null),
				new TestTable("trand",2,RandomDistributionModel.MODEL_NAME,null),
				new TestTable("orand",1,RandomDistributionModel.MODEL_NAME,null),
				new TestTable("tbroad",2,BroadcastDistributionModel.MODEL_NAME,null),
				new TestTable("obroad",1,BroadcastDistributionModel.MODEL_NAME,null),
				new TestTable("trange",2,RangeDistributionModel.MODEL_NAME,"birange"),
				new TestTable("orange",1,RangeDistributionModel.MODEL_NAME,"unrange"),
		};
		
		for(int i = 0; i < tabs.length; i++) {
			TestTable tt = tabs[i];
			conn.execute(tt.getTableDeclaration());
			// insert test
			ArrayList<String[]> rowValues = new ArrayList<String[]>();
			for(int j = 0; j < fids.length; j++) {
				String[] values = new String[tt.getIDCols()];
				values[0] = fids[j];
				if (values.length == 2)
					values[1] = sids[j];
				rowValues.add(values);
				conn.execute(tt.buildInsert(values));
			}
			for(int j = 2; j < rowValues.size(); j++) {
				String[] values = rowValues.get(j);
				conn.execute(tt.buildUpdate(values));
			}
			assertEquals(conn.fetch(tt.buildPostUpdateSelect()).getResults().size(), 4);
			for(int j = 2; j < rowValues.size(); j++) {
				String[] values = rowValues.get(j);
				int expectedSize = 1;
				if (values[0] == NULL_VALUE && values.length == 1)
					expectedSize = 2;
				assertEquals(conn.fetch(tt.buildSelect(values)).getResults().size(), expectedSize);
			}
			for(int j = 2; j < rowValues.size(); j++) {
				String[] values = rowValues.get(j);
				conn.execute(tt.buildDelete(values));
				assertTrue(conn.fetch(tt.buildSelect(values)).getResults().size() == 0);
			}
		}
		
	}
	
	private static class TestTable {
		
		private String tabname;
		private int ncols;
		private String model;
		private String range;
		
		public TestTable(String name, int idcols, String model, String range) {
			tabname = name;
			this.model = model;
			this.range = range;
			ncols = idcols;
		}
		
		public int getIDCols() {
			return ncols;
		}
		
		public boolean distributesOnColumns() {
			return StaticDistributionModel.MODEL_NAME.equals(model) || RangeDistributionModel.MODEL_NAME.equals(model);
		}
		
		public String getTableDeclaration() {
			StringBuffer buf = new StringBuffer();
			buf.append("create table ").append(tabname).append(" (");
			if (ncols == 1)
				buf.append("`fid` int, `junk` varchar(20)");
			else
				buf.append("`fid` int, `sid` int, `junk` varchar(20)");
			buf.append(" ) ").append(model).append(" distribute");
			if (distributesOnColumns()) {
				buf.append(" on (");
				if (ncols == 1)
					buf.append("`fid`");
				else
					buf.append("`fid`,`sid`");
				buf.append(") ");
				if (range != null)
					buf.append(" using ").append(range);
			}
			return buf.toString();
		}
		
		public String buildInsert(String[] values) {
			StringBuffer buf = new StringBuffer();
			buf.append("insert into ").append(tabname).append(" (");
			if (ncols == 1)
				buf.append("fid,junk");
			else
				buf.append("fid,sid,junk");
			buf.append(") values (");
			String junkVal = null;
			if (ncols == 1) {
				buf.append(values[0]);
				junkVal = "'" + values[0] + "'";
			} else {
				buf.append(values[0]).append(",").append(values[1]);
				junkVal = "'" + values[0] + "/" + values[1] + "'";
			}
			buf.append(", ").append(junkVal).append(")");
			return buf.toString();
		}
		
		private String buildWhereClause(String[] values) {
			StringBuffer buf = new StringBuffer();
			buf.append(" where ");
			if (values[0] == NULL_VALUE)
				buf.append("fid is null");
			else
				buf.append("fid = ").append(values[0]);
			if (ncols != 1) {
				if (values[1] == NULL_VALUE)
					buf.append(" and sid is null ");
				else
					buf.append(" and sid = ").append(values[1]);
			}
			return buf.toString();
		}
		
		// always only against the first value
		public String buildSelect(String[] values) {
			return "select * from " + tabname + buildWhereClause(values); 
		}
		
		public String buildPostUpdateSelect() {
			return "select * from " + tabname + " where junk = 'updated'";
		}
		
		public String buildDelete(String[] values) {
			return "delete from " + tabname + buildWhereClause(values);
		}
		
		public String buildUpdate(String[] values) {
			return "update " + tabname + " set junk = 'updated' " + buildWhereClause(values);
		}
	}
	
	@Test
	public void testNullLiteralColumn() throws Throwable {
		
		conn.execute("create table `A` (`id` int unsigned not null, `col1` varchar(50) not null, `id2` int unsigned, `col2` varchar(10) ) random distribute");
		conn.execute("insert into `A` values (1, '1_one', 11, '1_two')");
		conn.execute("insert into `A` values (2, '2_one', 22, '2_two')");
		conn.execute("insert into `A` values (3, '3_one', 33, '3_two')");
		conn.execute("insert into `A` values (4, '4_one', 44, '4_two')");
		conn.execute("insert into `A` values (5, '5_one', 55, '5_two')");
		conn.execute("insert into `A` values (6, '6_one', 66, '6_two')");
		
		conn.assertResults("select id as firstId, col1 as column1, id2 as secondId, col2 as column2, null as mynullcol from A order by id", 
				br(nr,Long.valueOf(1), "1_one", Long.valueOf(11), "1_two", null,
				   nr,Long.valueOf(2), "2_one", Long.valueOf(22), "2_two", null,
				   nr,Long.valueOf(3), "3_one", Long.valueOf(33), "3_two", null,
				   nr,Long.valueOf(4), "4_one", Long.valueOf(44), "4_two", null,
				   nr,Long.valueOf(5), "5_one", Long.valueOf(55), "5_two", null,
				   nr,Long.valueOf(6), "6_one", Long.valueOf(66), "6_two", null));
		conn.assertResults("select id as firstId, col1 as column1, id2 as secondId, null as mynullcol, col2 as column2 from A order by id", 
				br(nr,Long.valueOf(1), "1_one", Long.valueOf(11), null, "1_two",
				   nr,Long.valueOf(2), "2_one", Long.valueOf(22), null, "2_two",
				   nr,Long.valueOf(3), "3_one", Long.valueOf(33), null, "3_two",
				   nr,Long.valueOf(4), "4_one", Long.valueOf(44), null, "4_two",
				   nr,Long.valueOf(5), "5_one", Long.valueOf(55), null, "5_two",
				   nr,Long.valueOf(6), "6_one", Long.valueOf(66), null, "6_two"));
		conn.assertResults("select id as firstId, col1 as column1, null as mynullcol, id2 as secondId, col2 as column2 from A order by id", 
				br(nr,Long.valueOf(1), "1_one", null, Long.valueOf(11), "1_two",
				   nr,Long.valueOf(2), "2_one", null, Long.valueOf(22), "2_two",
				   nr,Long.valueOf(3), "3_one", null, Long.valueOf(33), "3_two",
				   nr,Long.valueOf(4), "4_one", null, Long.valueOf(44), "4_two",
				   nr,Long.valueOf(5), "5_one", null, Long.valueOf(55), "5_two",
				   nr,Long.valueOf(6), "6_one", null, Long.valueOf(66), "6_two"));
		conn.assertResults("select id as firstId, null as mynullcol, col1 as column1, id2 as secondId, col2 as column2 from A order by id", 
				br(nr,Long.valueOf(1), null, "1_one", Long.valueOf(11), "1_two",
				   nr,Long.valueOf(2), null, "2_one", Long.valueOf(22), "2_two",
				   nr,Long.valueOf(3), null, "3_one", Long.valueOf(33), "3_two",
				   nr,Long.valueOf(4), null, "4_one", Long.valueOf(44), "4_two",
				   nr,Long.valueOf(5), null, "5_one", Long.valueOf(55), "5_two",
				   nr,Long.valueOf(6), null, "6_one", Long.valueOf(66), "6_two"));
	}
}
