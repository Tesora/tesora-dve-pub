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
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.NativeDatabaseDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PEDatabaseDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;

public class UseAffectedRowsTest extends SchemaMirrorTest {

	private static PEDDL buildMultiPEDDL() {
		PEDDL out = new PEDDL();
		StorageGroupDDL sgddl = new StorageGroupDDL("multi",3,"multig");
		out.withStorageGroup(sgddl)
			.withDatabase(new PEDatabaseDDL("multidb").withStorageGroup(sgddl))
			;
		return out;
	}

	private static PEDDL buildSinglePEDDL() {
		PEDDL out = new PEDDL();
		StorageGroupDDL sgddl = new StorageGroupDDL("single",1,"singleg");
		out.withStorageGroup(sgddl)
			.withDatabase(new PEDatabaseDDL("singledb").withStorageGroup(sgddl))
			;
		return out;
	}
	
	private static NativeDDL buildNativeDDL() {
		NativeDDL out = new NativeDDL();
		out.withDatabase(new NativeDatabaseDDL("nativedb"))
			;
		return out;
	}
	
	private static final ProjectDDL singleDDL = buildSinglePEDDL();
	private static final ProjectDDL multiDDL = buildMultiPEDDL();
	private static final NativeDDL nativeDDL = buildNativeDDL();

	@Override
	protected ProjectDDL getSingleDDL() {
		return singleDDL;
	}
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return multiDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	private static Properties getUseAffectedRowsOptions(boolean value) {
		Properties props = new Properties();
		props.setProperty("useAffectedRows",Boolean.toString(value));
		return props;
	}
		
	protected ConnectionResource createConnection(ProjectDDL p, boolean useAffectedRows) throws Throwable {
		if (p == getNativeDDL()) 
			return new DBHelperConnectionResource(getUseAffectedRowsOptions(useAffectedRows)); 
		else if (p == getSingleDDL() || p == getMultiDDL())
			return new PortalDBHelperConnectionResource(getUseAffectedRowsOptions(useAffectedRows));

		throw new PEException("Unsupported ProjectDDL type " + p.getClass());
	}
		
	@BeforeClass
	public static void setup() throws Throwable {
		ArrayList<ProjectDDL> exists = new ArrayList<ProjectDDL>();
		if (multiDDL != null) exists.add(multiDDL);
		if (singleDDL != null) exists.add(singleDDL);
		if (nativeDDL != null) exists.add(nativeDDL);
		PETest.projectSetup(exists.toArray(new ProjectDDL[0]));
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	@AfterClass
	public static void teardown() throws Throwable {
	}

	@Override
	@Before
	public void connect() throws Throwable {
	}

	@Test
	public void testUpdateUseAffectedRowsTrue() throws Throwable {
		testUpdates(true);
	}
	
	@Test
	public void testUpdateUseAffectedRowsFalse() throws Throwable {
		testUpdates(false);
	}

	@Test
	public void testInsertOnUpdateUseAffectedRowsTrue() throws Throwable {
		testInsertOnDuplicateKeyUpdate(true);
	}
	
	@Test
	public void testInsertOnUpdateUseAffectedRowsFalse() throws Throwable {
		testInsertOnDuplicateKeyUpdate(false);
	}
	
	void testUpdates(boolean useAffectedRows) throws Throwable {
		try {
			connectWithUseAffectedRows(useAffectedRows);

			resetDbState();
			
			//manually test first with single and reset the native state for the multi
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1 */ update uar_random set data = 0");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1a */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1 */ update uar_range set data = 0");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1b */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1 */ update uar_broadcast set data = 0");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1c */ select row_count()");

			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2 */ update uar_random set data = 0");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2a */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2 */ update uar_range set data = 0");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2b */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2 */ update uar_broadcast set data = 0");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2c */ select row_count()");
			
			resetDbState();
	
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 3 */ update uar_random set data = 0");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 3a */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 3 */ update uar_range set data = 0");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 3b */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 3 */ update uar_broadcast set data = 0");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 3c */ select row_count()");
	
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4 */ update uar_random set data = 0");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4a */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4 */ update uar_range set data = 0");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4b */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4 */ update uar_broadcast set data = 0");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4c */ select row_count()");
		} finally {
			if (nativeResource != null) nativeResource.destroy();
			if (checkResource != null) checkResource.destroy();
			if (sysResource != null) sysResource.destroy();
		}
	}

	void testInsertOnDuplicateKeyUpdate(boolean useAffectedRows) throws Throwable {
		try {
			connectWithUseAffectedRows(useAffectedRows);

			resetDbState();
			
			//manually test first with single and reset the native state for the multi
//			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1 */ insert into uar_random (id,data) values (4,4) on duplicate key update data = 99");
//			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1a */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1 */ insert into uar_range (id,data) values (4,4) on duplicate key update data = 99");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1b */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1 */ insert into uar_broadcast (id,data) values (4,4) on duplicate key update data = 99");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 1c */ select row_count()");

//			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2 */ insert into uar_random (id,data) values (1,5) on duplicate key update data = 99");
//			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2a */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2 */ insert into uar_range (id,data) values (1,5) on duplicate key update data = 99");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2b */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2 */ insert into uar_broadcast (id,data) values (1,5) on duplicate key update data = 99");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 2c */ select row_count()");

//			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 3 */ insert into uar_random (id,data) values (2,2) on duplicate key update data = 2");
//			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 3a */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 3 */ insert into uar_range (id,data) values (2,2) on duplicate key update data = 2");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 3b */ select row_count()");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 3 */ insert into uar_broadcast (id,data) values (2,2) on duplicate key update data = 2");
			testUpdateCount(getNativeConnection(), getSingleConnection(), "/* 3c */ select row_count()");

			resetDbState();
	
//			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4 */ insert into uar_random (id,data) values (4,4) on duplicate key update data = 99");
//			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4a */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4 */ insert into uar_range (id,data) values (4,4) on duplicate key update data = 99");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4b */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4 */ insert into uar_broadcast (id,data) values (4,4) on duplicate key update data = 99");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 4c */ select row_count()");

//			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 5 */ insert into uar_random (id,data) values (1,5) on duplicate key update data = 99");
//			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 5a */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 5 */ insert into uar_range (id,data) values (1,5) on duplicate key update data = 99");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 5b */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 5 */ insert into uar_broadcast (id,data) values (1,5) on duplicate key update data = 99");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 5c */ select row_count()");

//			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 6 */ insert into uar_random (id,data) values (2,2) on duplicate key update data = 2");
//			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 6a */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 6 */ insert into uar_range (id,data) values (2,2) on duplicate key update data = 2");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 6b */ select row_count()");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 6 */ insert into uar_broadcast (id,data) values (2,2) on duplicate key update data = 2");
			testUpdateCount(getNativeConnection(), getMultiConnection(), "/* 6c */ select row_count()");
		} finally {
			if (nativeResource != null) nativeResource.destroy();
			if (checkResource != null) checkResource.destroy();
			if (sysResource != null) sysResource.destroy();
		}
	}
	
	void connectWithUseAffectedRows(boolean testValue) throws Throwable {
		DBHelperConnectionResource nativeConn = (DBHelperConnectionResource) createConnection(nativeDDL, testValue);
		PortalDBHelperConnectionResource singleConn = (PortalDBHelperConnectionResource) createConnection(singleDDL, testValue);
		PortalDBHelperConnectionResource multiConn = (PortalDBHelperConnectionResource) createConnection(multiDDL, testValue);
			
		nativeResource = new TestResource(nativeConn,nativeDDL);
		checkResource = new TestResource(singleConn,singleDDL);
		sysResource = new TestResource(multiConn,multiDDL);
		ArrayList<TestResource> trs = new ArrayList<TestResource>();
		trs.add(nativeResource);
		trs.add(checkResource);
		trs.add(sysResource);
		for(TestResource tr : trs)
			tr.getDDL().create(tr);
		
		for(TestResource tr : trs) {
			List<MirrorTest> populate = new ArrayList<MirrorTest>();
			if (tr.getDDL() == nativeDDL) {
				populate.add(new StatementMirrorProc("create table if not exists uar_range (id int, data int, primary key (id))"));
			} else if (tr.getDDL() == singleDDL) {
				populate.add(new StatementMirrorProc("/*#dve create range if not exists int_range_single (int) persistent group " + singleDDL.getPersistentGroup().getName() + " */"));
				populate.add(new StatementMirrorProc("create table if not exists uar_range (id int, data int, primary key (id)) /*#dve range distribute on (`id`) using int_range_single */"));
			} else if (tr.getDDL() == multiDDL) {
				populate.add(new StatementMirrorProc("/*#dve create range if not exists int_range_multi (int) persistent group " + multiDDL.getPersistentGroup().getName() + " */"));
				populate.add(new StatementMirrorProc("create table if not exists uar_range (id int, data int, primary key (id)) /*#dve range distribute on (`id`) using int_range_multi */"));
			}
			populate.add(new StatementMirrorProc("create table if not exists uar_random (id int, data int, primary key (id)) /*#dve random distribute */"));
			populate.add(new StatementMirrorProc("create table if not exists uar_broadcast (id int, data int, primary key (id)) /*#dve broadcast distribute */"));

			populate.add(new StatementMirrorProc("truncate uar_random"));
			populate.add(new StatementMirrorProc("truncate uar_range"));
			populate.add(new StatementMirrorProc("truncate uar_broadcast"));

			populate.add(new StatementMirrorProc("insert into uar_random (id, data) values (1,1),(2,2),(3,3)"));
			populate.add(new StatementMirrorProc("insert into uar_range (id, data) values (1,1),(2,2),(3,3)"));
			populate.add(new StatementMirrorProc("insert into uar_broadcast (id, data) values (1,1),(2,2),(3,3)"));
			for(MirrorTest mt : populate)
				mt.execute(tr,null);
		}
	}

	void resetDbState() throws Throwable {
		List<MirrorTest> populate = new ArrayList<MirrorTest>();
		
		populate.add(new StatementMirrorProc("truncate uar_random"));
		populate.add(new StatementMirrorProc("truncate uar_range"));
		populate.add(new StatementMirrorProc("truncate uar_broadcast"));

		populate.add(new StatementMirrorProc("insert into uar_random (id, data) values (1,1),(2,2),(3,3)"));
		populate.add(new StatementMirrorProc("insert into uar_range (id, data) values (1,1),(2,2),(3,3)"));
		populate.add(new StatementMirrorProc("insert into uar_broadcast (id, data) values (1,1),(2,2),(3,3)"));

		runTest(populate);
	}

	void testUpdateCount(ConnectionResource conn1, ConnectionResource conn2, String stmt) throws Throwable {
		ResourceResponse rr1 = conn1.execute(stmt);
		ResourceResponse rr2 = conn2.execute(stmt);
		rr1.assertEqualResponse(stmt, rr2);
	}
}
