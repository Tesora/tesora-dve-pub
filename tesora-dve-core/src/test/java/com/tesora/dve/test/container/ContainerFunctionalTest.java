// OS_STATUS: public
package com.tesora.dve.test.container;

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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaMirrorTest;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class ContainerFunctionalTest extends SchemaMirrorTest {
	static final String CONTAINER_NAME = "my_container";
	static final String RANGE_NAME = "my_range";

	static final ProjectDDL testDDL =
			new PEDDL("checkdb",
					new StorageGroupDDL("check", 3, "checkg"),
					"schema");
	static final NativeDDL nativeDDL =
			new NativeDDL("nativedb");

	@Override
	protected ProjectDDL getMultiDDL() {
		return testDDL;
	}

	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@Override
	protected ConnectionResource createConnection(ProjectDDL p) throws Throwable {
		if (p == getNativeDDL())
			return new DBHelperConnectionResource();
		else if (p == getMultiDDL())
			return new PortalDBHelperConnectionResource();

		throw new PEException("Unsupported ProjectDDL type " + p.getClass());
	}

	@BeforeClass
	public static void setup() throws Throwable {
		setup(testDDL, null, nativeDDL, getPopulate());
	}

	@AfterClass
	public static void shutdown1() throws Exception {
		Thread.sleep(2000);
	}

	private static List<MirrorTest> getPopulate() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null)
					return null;

				ResourceResponse rr = null;
				boolean isPE = !nativeDDL.equals(mr.getDDL());

				if (isPE) {
					mr.getConnection().execute(
							"create range " + RANGE_NAME + " (int) PERSISTENT group "
									+ testDDL.getPersistentGroup().getName());
					mr.getConnection().execute(
							"CREATE CONTAINER " + CONTAINER_NAME + " PERSISTENT GROUP "
									+ testDDL.getPersistentGroup().getName() + " RANGE DISTRIBUTE USING " + RANGE_NAME);
				}

				String[] table_decls = new String[] {
						"CREATE TABLE A (id int, col2 varchar(10), primary key (id)) /*#dve DISCRIMINATE ON (id) USING CONTAINER "
								+ CONTAINER_NAME + " */",
						"CREATE TABLE B (id int, tbla_id int, tble_id int, primary key (id)) /*#dve CONTAINER DISTRIBUTE "
								+ CONTAINER_NAME + " */",
						"CREATE TABLE C (id int, tblb_id int, primary key (id)) /*#dve CONTAINER DISTRIBUTE "
								+ CONTAINER_NAME + " */",
						"CREATE TABLE D (dkey varchar(10), type int, primary key (dkey)) /*#dve CONTAINER DISTRIBUTE "
								+ CONTAINER_NAME + " */",
						"CREATE TABLE E (id int, col2 varchar(10), primary key (id)) /*#dve PERSISTENT GROUP "
								+ testDDL.getPersistentGroup().getName() + " BROADCAST DISTRIBUTE */"
				};

				for (String decl : table_decls) {
					rr = mr.getConnection().execute(decl);
				}

				return rr;
			}
		});

		return out;
	}

	@Test
	public void basicContainerTest() throws Throwable {
		List<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorProc("insert into E values (100,'Evalue100')"));
		out.add(new StatementMirrorProc("/*#dve using container " + CONTAINER_NAME + "(global) */"));
		out.add(new StatementMirrorProc("insert into A values (1,'value1')"));
		out.add(new StatementMirrorProc("insert into A values (2,'value2')"));
		out.add(new StatementMirrorProc("insert into A values (3,'value3')"));
		out.add(new StatementMirrorFun("select * from A order by id"));
		out.add(new StatementMirrorProc("/*#dve using container " + CONTAINER_NAME + "(1) */"));
		out.add(new StatementMirrorProc("insert into B values (1000, 1, 100 )"));
		out.add(new StatementMirrorProc("insert into C values (10000, 1000 )"));
		out.add(new StatementMirrorProc("insert into C values (10001, 1000 )"));
		out.add(new StatementMirrorProc("insert into D values ('Dkey1', 50 )"));
		out.add(new StatementMirrorFun(
				"select * from A,B,C,E where A.id=1 and A.id=B.tbla_id and B.id=C.tblb_id and E.id=B.tble_id"));
		out.add(new StatementMirrorProc("/*#dve using container " + CONTAINER_NAME + "(2) */"));
		out.add(new StatementMirrorProc("insert into B values (1001, 2, 100 )"));
		out.add(new StatementMirrorProc("insert into C values (10003, 1001 )"));
		out.add(new StatementMirrorProc("insert into C values (10004, 1001 )"));
		out.add(new StatementMirrorProc("insert into D values ('Dkey2', 50 )"));
		out.add(new StatementMirrorFun(
				"select * from A,B,C,E where A.id=2 and A.id=B.tbla_id and B.id=C.tblb_id and E.id=B.tble_id"));
		out.add(new StatementMirrorProc("/*#dve using container " + CONTAINER_NAME + "(1) */"));
		out.add(new StatementMirrorProc("delete from C where tblb_id=1000"));
		out.add(new StatementMirrorProc("delete from B where tbla_id=1"));
		out.add(new StatementMirrorFun("select * from B,C where B.tbla_id=1 and B.id=C.tblb_id"));
		out.add(new StatementMirrorProc("delete from A where id=1"));
		out.add(new StatementMirrorFun("select * from A,B,C where A.id=1 and A.id=B.tbla_id and B.id=C.tblb_id"));

		runTest(out);
	}

	@Test
	public void notSoBasicContainerTest() throws Throwable {
		List<MirrorTest> out = new ArrayList<MirrorTest>();

		out.add(new StatementMirrorProc(
				"/*#dve create range uuid_range (binary(36)) persistent group " + testDDL.getPersistentGroup().getName()
						+ " */"));
		out.add(new StatementMirrorProc("/*#dve create container uuid_container persistent group "
				+ testDDL.getPersistentGroup().getName() + " range distribute using uuid_range */"));
		out.add(new StatementMirrorProc(
				"create table aa (id binary(36), col2 varchar(10)) /*#dve discriminate on (id) using container uuid_container */"));
		out.add(new StatementMirrorProc(
				"create table bb ( id int, tbla_id binary(36), tble_id int) /*#dve container distribute uuid_container */"));
		out.add(new StatementMirrorProc(
				"create table cc ( id int, tblb_id int) /*#dve container distribute uuid_container */"));
		out.add(new StatementMirrorProc(
				"create table dd ( dkey varchar(10), type int) /*#dve container distribute uuid_container */"));
		out.add(new StatementMirrorProc(
				"create table ee (id int, `desc` varchar(10)) /*#dve persistent group " + testDDL.getPersistentGroup().getName() + " broadcast distribute */"));
		out.add(new StatementMirrorProc("insert into ee values ( 17, 'seventeen' )"));
		out.add(new StatementMirrorProc("insert into ee values ( 18, 'eighteen' )"));
		out.add(new StatementMirrorProc("insert into ee values ( 19, 'nineteen' )"));
		out.add(new StatementMirrorProc("insert into ee values ( 20, 'twenty' )"));
		out.add(new StatementMirrorProc("/*#dve using container " + CONTAINER_NAME + "(global) */"));
		out.add(new StatementMirrorProc("insert into aa values ( '1ecd9f41-abde-11df-aaf6-1231390955c1', 'ten' )"));
		out.add(new StatementMirrorProc("insert into aa values ( '3bc71ae4-a739-11df-aaf6-1231390955c1', 'eleven' )"));
		out.add(new StatementMirrorProc("insert into aa values ( '50d4ad54-0240-11e0-b753-123139042631', 'twelve' )"));
		out.add(new StatementMirrorProc("insert into aa values ( '4fb6e00a-65f4-11e0-b753-123139042631', 'thirteen' )"));
		out.add(new StatementMirrorProc(
				"/*#dve using container uuid_container ('1ecd9f41-abde-11df-aaf6-1231390955c1') */"));
		out.add(new StatementMirrorProc("insert into bb values ( 1, '1ecd9f41-abde-11df-aaf6-1231390955c1', 17)"));
		out.add(new StatementMirrorProc("insert into cc values ( 1, 1 )"));
		out.add(new StatementMirrorProc(
				"/*#dve using container uuid_container('3bc71ae4-a739-11df-aaf6-1231390955c1') */"));
		out.add(new StatementMirrorProc("insert into bb values ( 2, '3bc71ae4-a739-11df-aaf6-1231390955c1', 18)"));
		out.add(new StatementMirrorProc("insert into cc values ( 2, 2 )"));
		out.add(new StatementMirrorProc(
				"/*#dve using container uuid_container('50d4ad54-0240-11e0-b753-123139042631') */"));
		out.add(new StatementMirrorProc("insert into bb values ( 3, '50d4ad54-0240-11e0-b753-123139042631', 18)"));
		out.add(new StatementMirrorProc("insert into cc values ( 3, 3 )"));
		out.add(new StatementMirrorProc(
				"/*#dve using container uuid_container('4fb6e00a-65f4-11e0-b753-123139042631') */"));
		out.add(new StatementMirrorProc("insert into bb values ( 4, '4fb6e00a-65f4-11e0-b753-123139042631', 18)"));
		out.add(new StatementMirrorProc("insert into cc values ( 4, 4 )"));
		out.add(new StatementMirrorProc(
				"/*#dve using container uuid_container('4fb6e00a-65f4-11e0-b753-123139042631') */"));

		out.add(new StatementMirrorFun(
				"select aa.id, aa.col2, bb.*, cc.*, ee.desc from aa, bb, cc, ee where aa.id = bb.tbla_id and bb.id = cc.tblb_id and bb.tble_id = ee.id and aa.id = '4fb6e00a-65f4-11e0-b753-123139042631' order by aa.id"));
		out.add(new StatementMirrorProc(
				"/*#dve using container uuid_container('50d4ad54-0240-11e0-b753-123139042631') */"));
		out.add(new StatementMirrorFun(
				"select aa.id, aa.col2, bb.*, cc.*, ee.desc from aa, bb, cc, ee where aa.id = bb.tbla_id and bb.id = cc.tblb_id and bb.tble_id = ee.id and aa.id = '50d4ad54-0240-11e0-b753-123139042631' order by aa.id"));
		out.add(new StatementMirrorProc(
				"/*#dve using container uuid_container('50d4ad54-0240-11e0-b753-123139042631') */"));
		out.add(new StatementMirrorFun(
				"select aa.id, aa.col2, bb.*, cc.*, ee.desc from aa, bb, cc, ee where aa.id = bb.tbla_id and bb.id = cc.tblb_id and bb.tble_id = ee.id and aa.id = '50d4ad54-0240-11e0-b753-123139042631' order by aa.id"));
		out.add(new StatementMirrorProc(
				"/*#dve using container uuid_container ('4fb6e00a-65f4-11e0-b753-123139042631') */"));
		out.add(new StatementMirrorFun(
				"select aa.id, aa.col2, bb.*, cc.*, ee.desc from aa, bb, cc, ee where aa.id = bb.tbla_id and bb.id = cc.tblb_id and bb.tble_id = ee.id and aa.id = '4fb6e00a-65f4-11e0-b753-123139042631' order by aa.id"));
		out.add(new StatementMirrorProc("/*#dve using container " + CONTAINER_NAME + "(global) */"));
		out.add(new StatementMirrorFun(
				"select aa.id, aa.col2, bb.*, cc.*, ee.desc from aa, bb, cc, ee where aa.id = bb.tbla_id and bb.id = cc.tblb_id and bb.tble_id = ee.id order by aa.id"));

		runTest(out);
	}
}
