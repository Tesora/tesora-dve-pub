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
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class StrictForeignKeyTest extends SchemaTest {

    private static final StorageGroupDDL checkSG = new StorageGroupDDL("check",2,"checkg");

	private static final ProjectDDL checkDDL =
		new PEDDL("adb",checkSG,"database");
	private static final ProjectDDL otherDDL =
			new PEDDL("otherdb",
					new StorageGroupDDL("other",2,"otherg"),
					"database");
	private static final ProjectDDL checkMateDDL =
			new PEDDL("icannt",checkSG, "database");
	
	
	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL, otherDDL, checkMateDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}
	
	public static interface TestConnections {
		
		public ConnectionResource getRootConnection();
		public ConnectionResource getTestConnection();
		public ProjectDDL getDDL();
		public Class<?> getExceptionClass();
		public void assertSchemaException(Throwable t, String message) throws Throwable;
		public String getDBName();
		
	}
	
	private static class LocalTestConnections implements TestConnections {
		
		private ConnectionResource conn;
		
		public LocalTestConnections(ConnectionResource cr) {
			conn = cr;
		}

		@Override
		public ConnectionResource getRootConnection() {
			return conn;
		}

		@Override
		public ConnectionResource getTestConnection() {
			return conn;
		}

		@Override
		public ProjectDDL getDDL() {
			return checkDDL;
		}

		@Override
		public Class<?> getExceptionClass() {
			return PEException.class;
		}

		@Override
		public void assertSchemaException(Throwable t, String message) throws Throwable {
			SchemaTest.assertSchemaException(t, message);
		}

		@Override
		public String getDBName() {
			return checkDDL.getDatabaseName();
		}
		
	}
	
	public static void testColocation(TestConnections conns, String[] containerDists, Object[] shouldPass) throws Throwable {
		String[] rhsTabNames = new String[] {
				"B",
				"Ra",
				"Rb",
				"A"
		};

		String[] rhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`id`) using ra",
				"range distribute on (`id`) using rb",
				"random distribute"
		};
		conns.getRootConnection().execute("create range ra (int) persistent group " + conns.getDDL().getPersistentGroup().getName());
		conns.getRootConnection().execute("create range rb (int) persistent group " + conns.getDDL().getPersistentGroup().getName());
		String rbody = "(`id` int, `junk` varchar(32), primary key (`id`)) ";
		// create the rhs tables
		for(int i = 0; i < rhsTabNames.length; i++) {
			String decl = "create table R" + rhsTabNames[i] + rbody + rhsDists[i];
			echo(decl);
			conns.getTestConnection().execute(decl);
		}
		// now try the lhs tests
		// first index is the referring dist, second is the target dist -
		// assuming the referring table is distributed on the fk - not the pk
		for(int i = 0; i < rhsTabNames.length; i++) {
			for(int j = 0; j < rhsDists.length; j++) {
				StringBuffer buf = new StringBuffer();
				buf.append("create table L").append(rhsTabNames[j]).append("R").append(rhsTabNames[i])
				.append(" (`id` int, `fid` int, ");
				buf.append("foreign key (`fid`) references R").append(rhsTabNames[i]).append(" (`id`)) engine=innodb ");
				buf.append(containerDists[j]);
				int[] spec = new int[] { j, i };
				boolean matches = false;
				for(int k = 0; k < shouldPass.length; k++) {
					int[] candidate = (int[]) shouldPass[k];
					if (candidate[0] == spec[0] && candidate[1] == spec[1]) {
						matches = true;
						break;
					}
				}
				try {
					echo(matches + " : " + buf.toString());
					conns.getTestConnection().execute(buf.toString());
					if (!matches)
						fail("should have failed: " + buf.toString());
				} catch (Throwable t) {
					if (conns.getExceptionClass().isInstance(t)) {
						if (matches)
							throw t;
					} else {
						throw t;
					}
				}  
			}
		}
	}
	
	public static void testColocated(TestConnections conns) throws Throwable {
		String[] lhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`fid`) using ra",
				"range distribute on (`fid`) using rb",
				"random distribute"			
		};
		// now try the lhs tests
		// first index is the referring dist, second is the target dist -
		// assuming the referring table is distributed on the fk - not the pk
		Object[] shouldPass = new Object[] {
				new int[] { 0,0 }, new int[] { 1, 0 }, new int[] { 2, 0 }, new int[] { 3, 0 },
				new int[] { 1,1 }, new int[] { 2, 2 }
		};
		testColocation(conns, lhsDists, shouldPass);		
	}
	
	@Test
	public void testColocated() throws Throwable {
		ConnectionResource conn = new ProxyConnectionResource();
		try {
			checkDDL.create(conn);
			testColocated(new LocalTestConnections(conn));
		} finally {
			checkDDL.destroy(conn);
			conn.disconnect();
			conn = null;
		}
	}

	public static void testNonColocated(TestConnections conns) throws Throwable {
		String[] lhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`id`) using ra",
				"range distribute on (`id`) using rb",
				"random distribute"			
		};
		Object[] shouldPass = new Object[] {
				new int[] { 0,0 }, new int[] { 1, 0 }, new int[] { 2, 0 }, new int[] { 3, 0 }
		};
		testColocation(conns, lhsDists,shouldPass);
	}
	
	@Test
	public void testNonColocated() throws Throwable {
		ConnectionResource conn = new ProxyConnectionResource();
		try {
			checkDDL.create(conn);
			testNonColocated(new LocalTestConnections(conn));
		} finally {
			checkDDL.destroy(conn);
			conn.disconnect();
			conn = null;
		}
	}

	public static void testDropTargetTable(TestConnections conns) throws Throwable {
		String[] rhsTabNames = new String[] {
				"RB",
				"RRa"
		};
		String[] rhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`id`) using ra"
		};
		String[] lhsTabNames = new String[] { 
				"LB",
				"LRa",
				"LA"
		};
		String[] lhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`fid`) using ra",
				"random distribute"
		};
		conns.getRootConnection().execute("create range ra (int) persistent group " + conns.getDDL().getPersistentGroup().getName());
		String rbody = "(`id` int, `junk` varchar(32), primary key (`id`)) ";
		// create the rhs tables
		for(int i = 0; i < rhsTabNames.length; i++) 
			conns.getTestConnection().execute("create table " + rhsTabNames[i] + rbody + rhsDists[i]);
		// create the lhs tables
		String lbody = " (`id` int, `fid` int, foreign key (`fid`) references ";
		for(int li = 0; li < lhsTabNames.length; li++) {
			if (li == 0) 
				conns.getTestConnection().execute("create table " + lhsTabNames[li] + lbody + "RB (`id`)) " + lhsDists[li]);
			// LRa - can target bcast and RRa
			// LA - can target bcast
			else if (li == 1) {
				for(int ri = 0; ri < rhsTabNames.length; ri++) {
					conns.getTestConnection().execute("create table " + lhsTabNames[li] + rhsTabNames[ri] + lbody + rhsTabNames[ri] + " (`id`)) " + lhsDists[li]);
				}
			} else if (li == 2) {
				conns.getTestConnection().execute("create table " + lhsTabNames[li] + lbody + "RB (`id`)) " + lhsDists[li]);
			}
		}
		// first off - dropping any of the rhs tables should result in an error
		for(int i = 0; i < rhsTabNames.length; i++) {
			try {
				conns.getTestConnection().execute("drop table " + rhsTabNames[i]);
				fail("should not be able to drop ref'd table");
			} catch (Throwable t) {
				conns.assertSchemaException(t, "Unable to drop table `" + rhsTabNames[i] + "` because referenced by foreign keys");
			}
		}
		Object[] cols = br(nr,"LB","fid","RB","id",
				nr,"LRaRB","fid","RB","id",
				nr,"LRaRRa","fid","RRa","id",
				nr,"LA","fid","RB","id");
		String consql = "select table_name, column_name, referenced_table_name, referenced_column_name from information_schema.key_column_usage where referenced_column_name is not null and table_schema = '" + conns.getDBName() + "'";
		conns.getTestConnection().assertResults(consql,cols);
		conns.getTestConnection().execute("set foreign_key_checks=0");
		for(int i = 0; i < rhsTabNames.length; i++) {
			conns.getTestConnection().execute("drop table " + rhsTabNames[i]);
		}
		conns.getTestConnection().assertResults(consql,cols);
		
	}
	
	// drop support
	@Test
	public void testDropTargetTable() throws Throwable {
		ProxyConnectionResource conn = new ProxyConnectionResource();
		try {
			checkDDL.create(conn);
			testDropTargetTable(new LocalTestConnections(conn));
		} finally {
			checkDDL.destroy(conn);
			conn.disconnect();
			conn = null;			
		}		
	}
	
	public static void testAlterFKs(TestConnections conns) throws Throwable {
		String[] rhsTabNames = new String[] {
				"RB",
				"RRa"
		};
		String[] rhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`id`) using ra"
		};
		String[] lhsTabNames = new String[] { 
				"LB",
				"LRa",
				"LA"
		};
		String[] lhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`fid`) using ra",
				"random distribute"
		};
		conns.getRootConnection().execute("create range ra (int) persistent group " + conns.getDDL().getPersistentGroup().getName());
		String rbody = "(`id` int, `junk` varchar(32), primary key (`id`)) ";
		// create the rhs tables
		for(int i = 0; i < rhsTabNames.length; i++) 
			conns.getTestConnection().execute("create table " + rhsTabNames[i] + rbody + rhsDists[i]);
		// create the lhs tables - but not the fks yet
		// create the lhs tables
		String lbody = " (`id` int, `fid` int)";
		LinkedHashMap<String,Pair<String,String>> alters = new LinkedHashMap<String,Pair<String,String>>();
		int counter = 0;
		for(int li = 0; li < lhsTabNames.length; li++) {
			if (li == 0) {
				conns.getTestConnection().execute("create table " + lhsTabNames[li] + lbody + lhsDists[li]);
				String fkName = "pefkt" + counter++;
				alters.put(fkName,new Pair<String,String>(lhsTabNames[li],"alter table " + lhsTabNames[li] + " add constraint " + fkName + " foreign key (fid) references RB (`id`)")); 
			}
			// LRa - can target bcast and RRa
			// LA - can target bcast
			else if (li == 1) {
				for(int ri = 0; ri < rhsTabNames.length; ri++) {
					conns.getTestConnection().execute("create table " + lhsTabNames[li] + rhsTabNames[ri] + lbody + lhsDists[li]);
					String fkName = "pefkt" + counter++;
					String tn = lhsTabNames[li] + rhsTabNames[ri];
					alters.put(fkName, new Pair<String,String>(tn, "alter table " + tn + " add constraint " + fkName + " foreign key (fid) references " + rhsTabNames[ri] + "(id)")); 
				}
			} else if (li == 2) {
				conns.getTestConnection().execute("create table " + lhsTabNames[li] + lbody + lhsDists[li]);
				String fkName = "pefkt" + counter++;
				alters.put(fkName, new Pair<String,String>(lhsTabNames[li],"alter table " + lhsTabNames[li] + " add constraint " + fkName + " foreign key (fid) references RB (id)"));
			}
		}
		String consql = "select table_name, column_name, referenced_table_name, referenced_column_name from information_schema.key_column_usage where referenced_column_name is not null and table_schema = '" + checkDDL.getDatabaseName() + "'"; 
		conns.getTestConnection().assertResults(consql,br());			
		// these all should work
		for(Map.Entry<String, Pair<String,String>> me : alters.entrySet()) {
			conns.getTestConnection().execute(me.getValue().getSecond());
		}
		Object[] cols = br(nr,"LB","fid","RB","id",
				nr,"LRaRB","fid","RB","id",
				nr,"LRaRRa","fid","RRa","id",
				nr,"LA","fid","RB","id");
		conns.getTestConnection().assertResults(consql,cols);
		for(Map.Entry<String,Pair<String,String>> me : alters.entrySet()) {
			conns.getTestConnection().execute("alter table " + me.getValue().getFirst() + " drop foreign key " + me.getKey());
		}
		conns.getTestConnection().assertResults(consql,br());
		// test foreign_key_check=0
		for(String tn : rhsTabNames)
			conns.getTestConnection().execute("drop table " + tn);
		conns.getTestConnection().execute("set foreign_key_checks=0");
		// these all should work
		for(Map.Entry<String, Pair<String,String>> me : alters.entrySet()) {
			conns.getTestConnection().execute(me.getValue().getSecond());
		}
		conns.getTestConnection().assertResults(consql,cols);
		// even after we add the tables again
		for(int i = 0; i < rhsTabNames.length; i++) 
			conns.getTestConnection().execute("create table " + rhsTabNames[i] + rbody + rhsDists[i]);
		conns.getTestConnection().assertResults(consql,cols);			
		
	}
	
	// alter test
	@Test
	public void testAlterFKs() throws Throwable {
		ProxyConnectionResource conn = new ProxyConnectionResource();
		try {
			checkDDL.create(conn);
			testAlterFKs(new LocalTestConnections(conn));
		} finally {
			checkDDL.destroy(conn);
			conn.disconnect();
			conn = null;
		}		
	}	
	
	public static void testFKActions(TestConnections conns) throws Throwable {
		String[] rhsTabNames = new String[] {
				"RB",
				"RRa"
		};
		String[] rhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`id`) using ra"
		};
		String[] lhsTabNames = new String[] { 
				"LB",
				"LRa",
				"LA"
		};
		String[] lhsDists = new String[] {
				"broadcast distribute",
				"range distribute on (`fid`) using ra",
				"random distribute"
		};
		conns.getRootConnection().execute("create range ra (int) persistent group " + conns.getDDL().getPersistentGroup().getName());
		String rbody = "(`id` int, `junk` varchar(32), primary key (`id`)) ";
		// create the rhs tables
		for(int i = 0; i < rhsTabNames.length; i++) 
			conns.getTestConnection().execute("create table " + rhsTabNames[i] + rbody + rhsDists[i]);
		// actions we specifically care about
		// primitive actions
		ActionConfig deleteSetNull = new ActionConfig("on delete set null").withTestDelete().allowsBCastDelete();
		ActionConfig deleteCascade = new ActionConfig("on delete cascade").withTestDelete().allowsBCastDelete().allowsRegularDelete();
		ActionConfig updateSetNull = new ActionConfig("on update set null").withTestUpdate().allowsBCastUpdate();
		ActionConfig updateCascade = new ActionConfig("on update cascade").withTestUpdate().allowsBCastUpdate(); 

		ActionConfig[] actions = new ActionConfig[] {
				deleteSetNull,
				deleteCascade,
				updateSetNull,
				updateCascade,
				ActionConfig.combine(deleteSetNull, updateSetNull),
				ActionConfig.combine(deleteSetNull, updateCascade),
				ActionConfig.combine(deleteCascade, updateSetNull),
				ActionConfig.combine(deleteCascade, updateCascade)
		};
		// the kerns include the lhs tab name, the rhs tab name, the lhs dist
		// (we'll reuse the rhs tabs)
		List<String[]> kerns = new ArrayList<String[]>();
		kerns.add(new String[] { lhsTabNames[0], lhsDists[0], rhsTabNames[0]});
		kerns.add(new String[] { lhsTabNames[2], lhsDists[2], rhsTabNames[0]});
		for(int ri = 0; ri < rhsTabNames.length; ri++) {
			kerns.add(new String[] { lhsTabNames[1] + rhsTabNames[ri], lhsDists[1], rhsTabNames[ri] });
		}			
		String lbody = " (`id` int, `fid` int, foreign key (`fid`) references ";
		for(ActionConfig ac : actions) {
			// declare the tables
			for(String[] k : kerns) {
				String decl = "create table " + k[0] + lbody + k[2] + " (id) " + ac.getDecl() + ") " + k[1];
				conns.getTestConnection().execute(decl);
				// do the tests
				boolean childIsBCast = (k[0] == lhsTabNames[0]);
				if (ac.isTestUpdate()) try {
					String sql = "update " + k[2] + " set id = 1001 where junk = 'garbage'";
					conns.getTestConnection().execute(sql);
					// if we get here - test to see whether we should have
					if ((childIsBCast && ac.allowBCastUpdate()) || (!childIsBCast && ac.allowRegularUpdate())) {
						// ok
					} else {
						fail("should have thrown an exception on " + sql + ", referring " + decl);
					}
				} catch (Throwable t) {
					if ((childIsBCast && !ac.allowBCastUpdate()) || (!childIsBCast && !ac.allowRegularUpdate())) {
						// ok
						String message = "Unable to update table `" + k[2] + "` due to cascade/set null action on foreign key " + k[0] + "_ibfk_1 in table `" + k[0] + "`";
						conns.assertSchemaException(t, message);
					} else {
						throw t;
					}
				}
				if (ac.isTestDelete()) try {
					String sql = "delete from " + k[2] + " where junk = 'garbage'";
					conns.getTestConnection().execute(sql);
					if ((childIsBCast && ac.allowBCastDelete()) || (!childIsBCast && ac.allowRegularDelete())) {
						// ok
					} else {
						fail("should have thrown an exception on " + sql + ", referring " + decl);
					}
				} catch (Throwable t) {
					if ((childIsBCast && !ac.allowBCastDelete()) || (!childIsBCast && !ac.allowRegularDelete())) {
						// ok
						String message = "Unable to delete from `" + k[2] + "` due to set null action on foreign key " + k[0] + "_ibfk_1 in table `" + k[0] + "`";
						conns.assertSchemaException(t, message);
					} else {
						throw t;
					}					
				}
				// drop the table
				conns.getTestConnection().execute("drop table " + k[0]);
			}
		}						
		
	}
	
	@Test
	public void testFKActions() throws Throwable {
		ProxyConnectionResource conn = new ProxyConnectionResource();
		try {
			checkDDL.create(conn);
			testFKActions(new LocalTestConnections(conn));
		} finally {
			checkDDL.destroy(conn);
			conn.disconnect();
			conn = null;
		}		
	}
	
	private static class ActionConfig {
		
		private String decl;
		private boolean testDelete;
		private boolean testUpdate;
		private boolean bcastDelete;
		private boolean regularDelete;
		private boolean bcastUpdate;
		private boolean regularUpdate;
		
		public ActionConfig(String decl) {
			this.decl = decl;
			testDelete = false;
			testUpdate = false;
			bcastDelete = false;
			regularDelete = false;
			bcastUpdate = false;
			regularUpdate = false;
		}
		
		public String getDecl() { return decl; }
		public boolean isTestDelete() { return testDelete; }
		public boolean isTestUpdate() { return testUpdate; }
		public boolean allowBCastDelete() { return bcastDelete; }
		public boolean allowRegularDelete() { return regularDelete; }
		public boolean allowBCastUpdate() { return bcastUpdate; }
		public boolean allowRegularUpdate() { return regularUpdate; }
		
		public static ActionConfig combine(ActionConfig left, ActionConfig right) {
			ActionConfig out = new ActionConfig(left.getDecl() + " " + right.getDecl());
			out.testDelete = left.testDelete || right.testDelete;
			out.testUpdate = left.testUpdate || right.testUpdate;
			out.bcastDelete = left.bcastDelete || right.bcastDelete;
			out.regularDelete = left.regularDelete || right.regularDelete;
			out.bcastUpdate = left.bcastUpdate || right.bcastUpdate;
			out.regularUpdate = left.regularUpdate || right.regularUpdate;
			return out;
		}
		
		public ActionConfig withTestDelete() {
			testDelete = true;
			return this;
		}
		
		public ActionConfig withTestUpdate() {
			testUpdate = true;
			return this;
		}
		
		public ActionConfig allowsBCastDelete() {
			bcastDelete = true;
			return this;
		}
		
		public ActionConfig allowsRegularDelete() {
			regularDelete = true;
			return this;
		}
		
		public ActionConfig allowsBCastUpdate() {
			bcastUpdate = true;
			return this;
		}		
	}
	
	@Test
	public void testWeirdFKRules() throws Throwable {
		ProxyConnectionResource conn = new ProxyConnectionResource();
		try {
			checkDDL.create(conn);
			// just going to use a couple of bcast tables for this
			conn.execute("create table target (`id` int, `fid` int, primary key (`id`)) broadcast distribute");
			conn.assertResults("show keys in target", 
					br(nr,"target",I_ZERO,"PRIMARY",I_ONE,"id","A",getIgnore(),null,null,"","BTREE","",""));
			conn.execute("create table ref (`id` int, `fid` int, foreign key (`fid`) references `target` (`id`))");
//			System.out.println(conn.printResults("show keys in ref"));
			conn.assertResults("show keys in ref", 
					br(nr,"ref",I_ONE,"fid",I_ONE,"fid","A",getIgnore(),null,null,"YES","BTREE","",""));
			// make sure we named the foreign key correctly
			conn.assertResults("select constraint_name, constraint_type from information_schema.table_constraints where table_schema = '" + checkDDL.getDatabaseName() + "' and table_name = 'ref'",
					br(nr,"ref_ibfk_1","FOREIGN KEY"));
			// dropping the constraint should leave the key in place
			conn.execute("alter table ref drop foreign key `ref_ibfk_1`");
			conn.assertResults("select constraint_name, constraint_type from information_schema.table_constraints where table_schema = '" + checkDDL.getDatabaseName() + "' and table_name = 'ref'",
					br());
			conn.assertResults("show keys in ref", 
					br(nr,"ref",I_ONE,"fid",I_ONE,"fid","A",getIgnore(),null,null,"YES","BTREE","",""));
			// adding the key again should use the existing synthetic key
			conn.execute("alter table ref add foreign key (`fid`) references `target` (`id`)");
			conn.assertResults("select constraint_name, constraint_type from information_schema.table_constraints where table_schema = '" + checkDDL.getDatabaseName() + "' and table_name = 'ref'",
					br(nr,"ref_ibfk_1","FOREIGN KEY"));
			conn.assertResults("show keys in ref", 
					br(nr,"ref",I_ONE,"fid",I_ONE,"fid","A",getIgnore(),null,null,"YES","BTREE","",""));
			// adding a new key should cause the synthetic key to go away
			conn.execute("alter table ref add key `exp_key` (`fid`) using hash");
			conn.assertResults("show keys in ref", 
					br(nr,"ref",I_ONE,"exp_key",I_ONE,"fid","A",getIgnore(),null,null,"YES","HASH","",""));
		} finally {
			checkDDL.destroy(conn);
			conn.disconnect();
			conn = null;
		}
	}
	
	@Test
	public void testConstraintName() throws Throwable {
		ProxyConnectionResource conn = new ProxyConnectionResource();
		try {
			otherDDL.create(conn);
			checkDDL.create(conn);
			
			// just going to use a couple of bcast tables for this
			conn.execute("create table a (`id` int, primary key (`id`)) broadcast distribute");
			conn.execute("create table b (`id` int, primary key (`id`)) broadcast distribute");
			
			// expect this to fail
			try {
				conn.execute("create table c (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
						"primary key (`id`), " +
						"key sameindexname (kid1), " +
						"key sameindexname (kid2), " +
						"constraint sameconstraintname unique sameindexname (uid1), " +
						"constraint sameconstraintname unique sameindexname (uid2), " +
						"constraint sameconstraintname foreign key sameindexname (aid) references a (id), " +
						"constraint sameconstraintname foreign key sameindexname (bid) references b (id) " +
						") broadcast distribute");
				fail("Expected exception statement 1");
			} catch (Exception e) {
				assertTrue(StringUtils.containsIgnoreCase(e.getCause().getMessage(), "Duplicate key name: sameindexname"));
			}

			try {
				conn.execute("create table c (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
						"primary key (`id`), " +
						"key sameindexname (kid1), " +
						"key sameindexname2 (kid2), " +
						"constraint sameconstraintname unique sameindexname (uid1), " +
						"constraint sameconstraintname unique sameindexname (uid2), " +
						"constraint sameconstraintname foreign key sameindexname (aid) references a (id), " +
						"constraint sameconstraintname foreign key sameindexname (bid) references b (id) " +
						") broadcast distribute");
				fail("Expected exception statement 2");
			} catch (Exception e) {
				assertTrue(StringUtils.containsIgnoreCase(e.getCause().getMessage(), "Duplicate key name: sameindexname"));
			}

			try {
				conn.execute("create table c (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
						"primary key (`id`), " +
						"key sameindexname (kid1), " +
						"key sameindexname2 (kid2), " +
						"constraint sameconstraintname unique sameindexname (uid1), " +
						"constraint sameconstraintname unique sameindexname4 (uid2) " +
						") broadcast distribute");
				fail("Expected exception statement 3");
			} catch (Exception e) {
				assertTrue(StringUtils.containsIgnoreCase(e.getCause().getMessage(), "Duplicate key name: sameindexname"));
			}

			try {
				conn.execute("create table c (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
						"primary key (`id`), " +
						"key sameindexname (kid1), " +
						"key sameindexname2 (kid2), " +
						"constraint sameconstraintname unique sameindexname3 (uid1), " +
						"constraint sameconstraintname unique sameindexname4 (uid2), " +
						"constraint sameconstraintname foreign key sameindexname (aid) references a (id), " +
						"constraint sameconstraintname foreign key sameindexname (bid) references b (id) " +
						") broadcast distribute");
				fail("Expected exception statement 4");
			} catch (Exception e) {
				assertTrue(StringUtils.containsIgnoreCase(e.getCause().getMessage(), "Duplicate foreign key name: sameconstraintname"));
			}

			try {
				conn.execute("create table c (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
						"primary key (`id`), " +
						"key sameindexname (kid1), " +
						"key sameindexname2 (kid2), " +
						"constraint sameconstraintname unique sameindexname3 (uid1), " +
						"constraint sameconstraintname unique sameindexname4 (uid2), " +
						"constraint sameconstraintname foreign key sameindexname5 (aid) references a (id), " +
						"constraint sameconstraintname foreign key sameindexname6 (bid) references b (id) " +
						") broadcast distribute");
				fail("Expected exception statement 5");
			} catch (Exception e) {
				assertTrue(StringUtils.containsIgnoreCase(e.getCause().getMessage(), "Duplicate foreign key name: sameconstraintname"));
			}

			// this should succeed!
			conn.execute("create table c (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
					"primary key (`id`), " +
					"key sameindexname (kid1), " +
					"key sameindexname2 (kid2), " +
					"constraint sameconstraintname unique sameindexname3 (uid1), " +
					"constraint sameconstraintname unique sameindexname4 (uid2), " +
					"constraint sameconstraintname foreign key sameindexname (aid) references a (id), " +
					"constraint sameconstraintname2 foreign key sameindexname (bid) references b (id) " +
					") broadcast distribute");
			conn.assertResults("select constraint_name, constraint_type from information_schema.table_constraints where table_schema = 'adb' and table_name = 'c' order by constraint_name, constraint_type", 
					br(nr,"PRIMARY","PRIMARY KEY",
					   nr,"sameconstraintname","FOREIGN KEY",
					   nr,"sameconstraintname","UNIQUE",
					   nr,"sameconstraintname","UNIQUE",
					   nr,"sameconstraintname2","FOREIGN KEY"));
			conn.assertResults("show keys in c", 
					br(nr,"c",1,"aid",1,"aid","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"c",1,"bid",1,"bid","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"c",0,"PRIMARY",1,"id","A",BigInteger.valueOf(-1),null,null,"","BTREE","","",
					   nr,"c",1,"sameindexname",1,"kid1","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"c",1,"sameindexname2",1,"kid2","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"c",0,"sameindexname3",1,"uid1","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"c",0,"sameindexname4",1,"uid2","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","",""
					   ));
			
			// make sure FK constraint names are unique across the database
			try {
				conn.execute("create table d (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
						"primary key (`id`), " +
						"key sameindexname (kid1), " +
						"key sameindexname2 (kid2), " +
						"constraint sameconstraintname unique sameindexname3 (uid1), " +
						"constraint sameconstraintname unique sameindexname4 (uid2), " +
						"constraint sameconstraintname2 foreign key sameindexname (aid) references a (id), " +
						"constraint sameconstraintname4 foreign key sameindexname (bid) references b (id) " +
						") broadcast distribute");
				fail("Expected exception statement 6");
			} catch (Exception e) {
				assertTrue(StringUtils.containsIgnoreCase(e.getCause().getMessage(), "Duplicate foreign key name: sameconstraintname2"));
			}
			
			conn.execute("create table d (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
					"primary key (`id`), " +
					"key sameindexname (kid1), " +
					"key sameindexname2 (kid2), " +
					"constraint sameconstraintname unique sameindexname3 (uid1), " +
					"constraint sameconstraintname unique sameindexname4 (uid2), " +
					"constraint sameconstraintname3 foreign key sameindexname (aid) references a (id), " +
					"constraint sameconstraintname4 foreign key sameindexname (bid) references b (id) " +
					") broadcast distribute");
			conn.assertResults("select constraint_name, constraint_type from information_schema.table_constraints where table_schema = 'adb' and table_name = 'd' order by constraint_name, constraint_type", 
					br(nr,"PRIMARY","PRIMARY KEY",
					   nr,"sameconstraintname","UNIQUE",
					   nr,"sameconstraintname","UNIQUE",
					   nr,"sameconstraintname3","FOREIGN KEY",
					   nr,"sameconstraintname4","FOREIGN KEY"));
			conn.assertResults("show keys in d", 
					br(nr,"d",1,"aid",1,"aid","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"d",1,"bid",1,"bid","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"d",0,"PRIMARY",1,"id","A",BigInteger.valueOf(-1),null,null,"","BTREE","","",
					   nr,"d",1,"sameindexname",1,"kid1","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"d",1,"sameindexname2",1,"kid2","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"d",0,"sameindexname3",1,"uid1","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"d",0,"sameindexname4",1,"uid2","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","",""
					   ));

			// make sure the "standard" way still works
			conn.execute("create table e (`id` int, `aid` int, `bid` int, `uid1` int, `uid2` int, `kid1` int, `kid2` int, " +
					"primary key (`id`), " +
					"key (kid1), " +
					"key (kid2), " +
					"unique (uid1), " +
					"unique (uid2), " +
					"foreign key (aid) references a (id), " +
					"foreign key (bid) references b (id) " +
					") broadcast distribute");
			
			// test the case that started it all
			conn.execute("CREATE TABLE `cs` (`sid` smallint(5) unsigned NOT NULL, PRIMARY KEY (`sid`)) BROADCAST DISTRIBUTE");
			conn.execute("CREATE TABLE `tg` ( " +
					  "`tid` int(10) unsigned NOT NULL AUTO_INCREMENT, " +
					  "`nm` varchar(255) DEFAULT NULL, " +
					  "`st` smallint(6) NOT NULL DEFAULT '0', " +
					  "`fci` int(10) unsigned DEFAULT NULL, " +
					  "`fsi` smallint(5) unsigned DEFAULT NULL, " +
					  "PRIMARY KEY (`tid`) USING BTREE, " +
					  "KEY `FK_TG_FCICEEI` (`fci`), " +
					  "KEY `FK_TG_FSICSSI` (`fsi`), " +
					  "CONSTRAINT `FK_TG_FSICSSI` FOREIGN KEY (`fsi`) REFERENCES `cs` (`sid`) ON DELETE SET NULL ON UPDATE NO ACTION " +
					") ENGINE=InnoDB AUTO_INCREMENT=199 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC BROADCAST DISTRIBUTE");			
			conn.assertResults("select constraint_name, constraint_type from information_schema.table_constraints where table_schema = 'adb' and table_name = 'tg' order by constraint_name, constraint_type", 
					br(nr,"FK_TG_FSICSSI","FOREIGN KEY",
					   nr,"PRIMARY","PRIMARY KEY"));
			conn.assertResults("show keys in tg", 
					br(nr,"tg",1,"FK_TG_FCICEEI",1,"fci","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"tg",1,"FK_TG_FSICSSI",1,"fsi","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"tg",0,"PRIMARY",1,"tid","A",BigInteger.valueOf(-1),null,null,"","BTREE","",""
					   ));
			
			// make sure we can create the same table and constraint names in another database
			conn.execute("use otherdb");
			conn.execute("CREATE TABLE `cs` (`sid` smallint(5) unsigned NOT NULL, PRIMARY KEY (`sid`)) BROADCAST DISTRIBUTE");
			conn.execute("CREATE TABLE `tg` ( " +
					  "`tid` int(10) unsigned NOT NULL AUTO_INCREMENT, " +
					  "`nm` varchar(255) DEFAULT NULL, " +
					  "`st` smallint(6) NOT NULL DEFAULT '0', " +
					  "`fci` int(10) unsigned DEFAULT NULL, " +
					  "`fsi` smallint(5) unsigned DEFAULT NULL, " +
					  "PRIMARY KEY (`tid`) USING BTREE, " +
					  "KEY `FK_TAG_FSICSSI` (`fsi`), " +
					  "CONSTRAINT `FK_TAG_FSICSSI` FOREIGN KEY (`fsi`) REFERENCES `cs` (`sid`) ON DELETE SET NULL ON UPDATE NO ACTION " +
					") ENGINE=InnoDB AUTO_INCREMENT=199 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC BROADCAST DISTRIBUTE");			
			conn.assertResults("select constraint_name, constraint_type from information_schema.table_constraints where table_schema = 'otherdb' and table_name = 'tg' order by constraint_name, constraint_type", 
					br(nr,"FK_TAG_FSICSSI","FOREIGN KEY",
					   nr,"PRIMARY","PRIMARY KEY"));
			conn.assertResults("show keys in tg", 
					br(nr,"tg",1,"FK_TAG_FSICSSI",1,"fsi","A",BigInteger.valueOf(-1),null,null,"YES","BTREE","","",
					   nr,"tg",0,"PRIMARY",1,"tid","A",BigInteger.valueOf(-1),null,null,"","BTREE","",""
					   ));
		} finally {
			checkDDL.destroy(conn);
			otherDDL.destroy(conn);
			conn.disconnect();
			conn = null;
		}
	}
	
	@Test
	public void testPE1292() throws Throwable {
		String cn = checkDDL.getDatabaseName();
		String on = checkMateDDL.getDatabaseName();
		String ucn = "use " + cn;
		String uon = "use " + on;
		String refFmt = "select constraint_schema, table_name, unique_constraint_schema, referenced_table_name from information_schema.referential_constraints where constraint_schema = '%s' and table_name = '%s'";
		String colFmt = "select table_schema, table_name, column_name, referenced_table_schema, referenced_table_name, referenced_column_name from information_schema.key_column_usage where column_name = '%s'";
		ProxyConnectionResource conn = new ProxyConnectionResource();
		try {
			checkDDL.create(conn);
			checkMateDDL.create(conn);
			conn.execute(uon);
			conn.execute("create table targ (id int, primary key (id)) broadcast distribute");
			conn.execute(ucn);
			conn.execute("create table ref (id int, fid int, primary key (id), foreign key (fid) references " + on + ".targ (id)) broadcast distribute");
			conn.assertResults(String.format(refFmt,cn,"ref"),
					br(nr,cn,"ref",on,"targ"));
			conn.assertResults(String.format(colFmt,"fid"),
					br(nr,cn,"ref","fid",on,"targ","id"));
			String cts = AlterTest.getCreateTable(conn, "ref");
			conn.execute("set foreign_key_checks=0");
			conn.execute("drop table " + on + ".targ");
			conn.assertResults(String.format(refFmt,cn,"ref"),
					br(nr,cn,"ref",on,"targ"));
			conn.assertResults(String.format(colFmt,"fid"),
					br(nr,cn,"ref","fid",on,"targ","id"));
			conn.execute("create table " + on + ".targ (id int, primary key (id)) broadcast distribute");
			String ncts = AlterTest.getCreateTable(conn, "ref");
			assertEquals(cts,ncts);

			// now drop both and create in the other order
			conn.execute("drop table " + on + ".targ");
			conn.execute("drop table " + cn + ".ref");
			conn.execute(uon);
			conn.execute("create table " + cn + ".ref (id int, fid int, primary key (id), foreign key (fid) references targ (id)) broadcast distribute");
			conn.execute("create table targ (id int, primary key (id)) broadcast distribute");
			conn.assertResults(String.format(refFmt,cn,"ref"),
					br(nr,cn,"ref",on,"targ"));
			conn.assertResults(String.format(colFmt,"fid"),
					br(nr,cn,"ref","fid",on,"targ","id"));
			ncts = AlterTest.getCreateTable(conn, cn + ".ref");
			assertEquals(cts,ncts);


			conn.execute("drop table if exists " + cn + ".ref");
			conn.execute("drop table if exists " + on + ".targ");
		} finally {
			conn.execute("drop database " + on);
			checkDDL.destroy(conn);
			checkMateDDL.destroy(conn);
			conn.disconnect();
			conn = null;
		}

	}


}
