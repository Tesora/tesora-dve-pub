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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class GroupPolicyDDLTest extends SchemaTest {
	
	private static final ProjectDDL checkDDL =
		new PEDDL("checkdb",
				new StorageGroupDDL("check",1,"checkg"),
				"schema");

	@BeforeClass
	public static void setup() throws Exception {
		PETest.projectSetup(checkDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
	}

	private static final String userName = "regular";
	private static final String userAccess = "localhost";
	
	protected PortalDBHelperConnectionResource conn;
	protected PortalDBHelperConnectionResource userConn;
	protected DBHelperConnectionResource dbh;
	
	@Before
	public void connect() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
		checkDDL.create(conn);
		dbh = new DBHelperConnectionResource();
		removeUser(conn,userName,userAccess);
		conn.execute("create user '" + userName + "'@'" + userAccess + "' identified by '" + userName + "'");
		userConn = new PortalDBHelperConnectionResource(userName,userName);
	}
	
	@After
	public void disconnect() throws Throwable {
		if(userConn != null) {
			userConn.disconnect();
			userConn = null;
		}
		if(conn != null) {
			removeUser(conn,userName,userAccess);

			conn.disconnect();
			conn = null;
		}
		if(dbh != null) {
			dbh.disconnect();
			dbh = null;
		}
	}
	
	@Test
	public void test() throws Throwable {
		conn.execute("create dynamic site policy ftpol (aggregate count 1 provider 'OnPremise' pool 'poolconf') strict=off");
		conn.assertResults("show dynamic site policies like 'ftpol'",
				br(nr,"ftpol","0",
						"poolconf",1L,OnPremiseSiteProvider.DEFAULT_NAME,
						null,null,null,
						null,null,null,
						null,null,null));
		conn.execute("alter dynamic site policy ftpol change aggregate count 2 provider 'OnPremise' pool 'poolconf'");
		conn.assertResults("show dynamic site policies like 'ftpol'",
				br(nr,"ftpol","0",
						"poolconf",2L,OnPremiseSiteProvider.DEFAULT_NAME,
						null,null,null,
						null,null,null,
						null,null,null));
		conn.execute("alter dynamic site policy ftpol change large count 15 provider 'OnPremise' pool 'excessive'");
		conn.assertResults("show dynamic site policies like 'ftpol'",
				br(nr,"ftpol","0",
						"poolconf",2L,OnPremiseSiteProvider.DEFAULT_NAME,
						null,null,null,
						null,null,null,
						"excessive",15L,OnPremiseSiteProvider.DEFAULT_NAME));
		conn.execute("alter dynamic site policy ftpol set strict=on change large count 3 provider 'OnPremise' pool 'restrained', small count 15 provider 'OnPremise' pool 'retained'");
		conn.assertResults("show dynamic site policies like 'ftpol'",
				br(nr,"ftpol","1",
						"poolconf",2L,OnPremiseSiteProvider.DEFAULT_NAME,
						"retained",15L,OnPremiseSiteProvider.DEFAULT_NAME,
						null,null,null,
						"restrained",3L,OnPremiseSiteProvider.DEFAULT_NAME));
		conn.execute("drop dynamic site policy ftpol");
		conn.assertResults("show dynamic site policy ftpol",br());
		conn.execute("create dynamic site policy rgpol (large count 1000 provider 'Loaded', aggregate count 1 provider 'Loaded', small count 10 provider 'Loaded', medium count 100 provider 'Loaded') strict=ON");
		conn.assertResults("show dynamic site policy rgpol", 
				br(nr,"rgpol","1",
						null,1L,"Loaded",
						null,10L,"Loaded",
						null,100L,"Loaded",
						null,1000L,"Loaded"));
		conn.execute("create dynamic site policy empty () strict=on");
		conn.assertResults("show dynamic site policy empty",
				br(nr,"empty","1",
						null,null,null,
						null,null,null,
						null,null,null,
						null,null,null));
		try {
			userConn.execute("show dynamic site policies");
		} catch (PEException e) {
			assertSchemaException(e,"Internal error: You do not have permission to query policies");
		}
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {

			userConn.execute("create dynamic site policy mine () strict = off");

			}
		}.assertError(SQLException.class, MySQLErrors.internalFormatter,
					"Internal error: You do not have permission to create a dynamic site policy");
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {

			userConn.execute("alter dynamic site policy mine change aggregate count 1");

			}
		}.assertError(SQLException.class, MySQLErrors.internalFormatter,
					"Internal error: You do not have permission to alter a dynamic site policy");
		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {

			userConn.execute("drop dynamic site policy empty");

			}
		}.assertError(SQLException.class, MySQLErrors.internalFormatter,
					"Internal error: You do not have permission to drop a dynamic site policy");
		
	}
			
}
