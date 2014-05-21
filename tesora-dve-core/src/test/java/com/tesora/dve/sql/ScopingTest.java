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
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.transform.strategy.SingleSiteStorageGroupTransformFactory;
import com.tesora.dve.sql.util.MirrorFun;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class ScopingTest extends SchemaMirrorTest {

	private static final ProjectDDL sysDDL =
		new PEDDL("sysdbst",
				new StorageGroupDDL("sys",1,"sysgst"),
				"schema");
	// turned off the adaptive test - since we don't create a tenant it's unclear how this
	// should behave
	private static final ProjectDDL checkDDL =
		new PEDDL("checkdbmt",
				new StorageGroupDDL("check",1,"checkgst"),
				"schema").withMTMode(MultitenantMode.ADAPTIVE);
	private static final NativeDDL nativeDDL =
		new NativeDDL("cdb");
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getSingleDDL() {
//		return checkDDL;
		return null;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	
	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL, checkDDL, nativeDDL, Collections.EMPTY_LIST);
	}

	static String buildTableName(ProjectDDL ddl, String n) {
		return ddl.getDatabaseName() + "." + n;
	}

	@Override
	protected void onConnect(TestResource tr) throws Throwable {
		// do nothing, we'll arrange for the use stmts as needed
	}

	@Before
	public void disablePlanCache() throws Throwable {
		try (ProxyConnectionResource pcr = new ProxyConnectionResource()) {
			pcr.execute("alter dve set " + CacheSegment.PLAN.getVariableName() + " = 0");
		}
	}

	@After
	public void enablePlanCache() throws Throwable {
		try (ProxyConnectionResource pcr = new ProxyConnectionResource()) {
			pcr.execute("alter dve set " + CacheSegment.PLAN.getVariableName() + " = " + CacheSegment.PLAN.getDefaultValue());
		}
	}

	@Test
	public void testUnsetDB() throws Throwable {
		List<MirrorTest> tests = buildStmts();
		runTest(tests);
		SingleSiteStorageGroupTransformFactory.disable(true);
		try {
			runTest(tests);
		} finally {
			SingleSiteStorageGroupTransformFactory.disable(false);
		}
	}
	
	@Test
	public void testSetDB() throws Throwable {
		TestResource[] resources = new TestResource[] { sysResource, checkResource, nativeResource };
		for(TestResource tr : resources)
			if (tr != null)
				tr.getConnection().execute("use " + tr.getDDL().getDatabaseName());
		List<MirrorTest> tests = buildStmts();
		runTest(tests);
		SingleSiteStorageGroupTransformFactory.disable(true);
		try {
			runTest(tests);
		} finally {
			SingleSiteStorageGroupTransformFactory.disable(false);
		}
	}
	
	@Test
	public void testSetDifferentDB() throws Throwable {
		TestResource[] resources = new TestResource[] { sysResource, checkResource, nativeResource };
		for(TestResource tr : resources)
			if (tr != null)
				tr.getConnection().execute("use mysql");
		List<MirrorTest> tests = buildStmts();
		runTest(tests);
		SingleSiteStorageGroupTransformFactory.disable(true);
		try {
			runTest(tests);
		} finally {
			SingleSiteStorageGroupTransformFactory.disable(false);
		}
	}
	
	private static List<MirrorTest> buildStmts() {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new MirrorProc("create table stest") {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				String ctabstmt = "create table " + buildTableName(mr.getDDL(), "stest")+ " (`id` int auto_increment, `gunk` varchar(32), primary key (`id`))";
				return mr.getConnection().execute(ctabstmt);
			}
			
		});
		tests.add(new MirrorProc("insert into stest") {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				String tabname = buildTableName(mr.getDDL(), "stest") ;
				return mr.getConnection().execute("insert into " + tabname + " (`gunk`) values ('" + mr.getDDL().getDatabaseName() + "')");				
			}
			
		});
		tests.add(new MirrorFun(false, true)  {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				String tabname = buildTableName(mr.getDDL(), "stest") ;
				return mr.getConnection().fetch("select id from " + tabname + " order by id");
			}
			
		});
		tests.add(new MirrorProc("update stest") {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				String tabname = buildTableName(mr.getDDL(), "stest") ;
				return mr.getConnection().execute("update " + tabname + " set gunk = concat(gunk,'s') where gunk = '" + mr.getDDL().getDatabaseName() + "'");
			}
			
		});
		// todo: investigate why this fails with adaptive now		
		tests.add(new MirrorProc("alter table stest") {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				String tabname = buildTableName(mr.getDDL(), "stest");
				ResourceResponse rr = mr.getConnection().execute("alter table " + tabname + " add index stupid (`gunk`)");
				return rr;
			}
			
		});
		/*
		tests.add(new MirrorFun(false, true) {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				return mr.getConnection().fetch("show tables in " + mr.getDDL().getDatabaseName() + " like 'stest'");
			}
			
		});
		*/
		tests.add(new MirrorProc("drop table stest") {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				String tabname = buildTableName(mr.getDDL(), "stest");
				ResourceResponse rr = mr.getConnection().execute("drop table " + tabname);
				return rr;
			}
			
		});

		return tests;
	}
	
	@Test
	public void testAmbiguity() throws Throwable {
		List<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("create table t1 (a int, b int)"));
		tests.add(new StatementMirrorProc("create table t2 (a int, b int)"));
		tests.add(new StatementMirrorFun("select t1.a, t2.b from t1, t2 where t1.b = t2.a order by a, b"));
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				// this should fail
				try {
					mr.getConnection().execute("select t1.a, t2.a, t2.b from t1, t2 where t1.b = t2.a order by a, b");
					fail("Not supposed to work");
				} catch (Throwable t) {
					// ok
				}
				return null;
			}
		});
		tests.add(new StatementMirrorFun("select t1.a, t2.a, t2.b from t1, t2 where t1.b = t2.a order by t1.a, t2.b"));
		tests.add(new StatementMirrorProc("drop table t1"));
		tests.add(new StatementMirrorProc("drop table t2"));
		// tests.add(new StatementMirrorProc("insert into t1 values (1,2)"));

		
		TestResource[] resources = new TestResource[] { sysResource, checkResource, nativeResource };
		for(TestResource tr : resources)
			if (tr != null)
				tr.getConnection().execute("use " + tr.getDDL().getDatabaseName());
		runTest(tests);
	}
}
