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

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class TriggerDDLTest extends SchemaTest {

	private static final ProjectDDL checkDDL =
			new PEDDL("adb",
					new StorageGroupDDL("check",1,"checkg"),
					"database");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}


	private PortalDBHelperConnectionResource conn;

	@Before
	public void before() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
		checkDDL.create(conn);
	}

	@After
	public void after() throws Throwable {
		checkDDL.destroy(conn);
		conn.close();
		conn = null;
	}

	@Test
	public void testCreate() throws Throwable {
		conn.execute("create table A (id int auto_increment, event varchar(32), primary key (id)) broadcast distribute");
		conn.execute("create table B (a int, b int, c int, primary key(a)) broadcast distribute");
		// this should work
		conn.execute("insert into B (a,b,c) values (1,1,1),(2,2,2)");
		conn.execute("create trigger btrig after insert on B for each row insert into A (event) values ('insert')");
//		System.out.println(conn.printResults("show triggers"));
		conn.assertResults("show triggers",
				br(nr,"btrig","INSERT","B","INSERT INTO `A` (`A`.`event`) VALUES ('insert')",
					"AFTER",null,"NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES","root@%",
					"utf8","utf8_general_ci","utf8_general_ci"));
//		System.out.println(conn.printResults("show create trigger btrig"));
		conn.assertResults("show create trigger btrig",
				br(nr,"btrig","NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES",
						"CREATE DEFINER=`root`@`%` trigger btrig after insert on B for each row insert into A (event) values ('insert')",
						"utf8","utf8_general_ci","utf8_general_ci"));
//		System.out.println(conn.printResults("select * from information_schema.triggers where trigger_schema = 'adb'"));
		conn.assertResults("select * from information_schema.triggers where trigger_schema = 'adb'",
				br(nr,"def","adb","btrig","INSERT","def","adb","B",0,null,
						"INSERT INTO `A` (`A`.`event`) VALUES ('insert')",
						"ROW","AFTER",null,null,"OLD","NEW",null,
						"NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES","root@%","utf8","utf8_general_ci","utf8_general_ci"));
		// this should fail
		new ExpectedExceptionTester() {

			@Override
			public void test() throws Throwable {
				conn.execute("insert into B (a,b,c) values (3,3,3)");
			}
			
		}.assertException(SQLException.class, "PEException: No support for trigger execution");
		// but, this should not fail
		conn.execute("delete from B where a = 1");
	}
	
}
