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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.MysqlConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class PrepStmtTest extends SchemaTest {

	private static StorageGroupDDL sg = new StorageGroupDDL("sys", 5, 2, "sysg");

	private static final ProjectDDL sysDDL = new PEDDL("prep", sg, "database");
	
	static ProxyConnectionResource pcr;
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(sysDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		pcr = new ProxyConnectionResource();
		sysDDL.create(pcr);
	}

	@Before
	public void beforeSetup() throws Throwable {
		pcr.execute("create table foo(col1 int)");
		pcr.execute("create table bar(col1 int, col2 int, col3 int)");
	}
	
	@After
	public void teardown() throws Throwable {
		pcr.execute("drop table if exists foo");
		pcr.execute("drop table if exists bar");
	}
	
	@AfterClass
	public static void uberTeardown() throws Throwable {
		pcr.disconnect();
	}
	
	@Test
	public void prepstmtTestMysqlConn() throws Throwable {
		MysqlConnectionResource conn = new MysqlConnectionResource();
		executeTest(conn);
		conn.disconnect();
	}

	@Test
	public void paramLimitTest() throws Throwable {
		@SuppressWarnings("resource")
		MysqlConnectionResource conn = new MysqlConnectionResource();
		conn.execute(null, "use prep");
		
		conn.execute("truncate foo");

		int paramCount = 65535;
		StringBuilder query = new StringBuilder();
		query.append("INSERT into foo VALUES ");
		List<Object> params = new ArrayList<Object>();
		for (int i = 0; i < paramCount; i++) {
			query.append("(?)");
			if ( i < paramCount - 1 )
				query.append(",");
			params.add(new Short((short) 2));
		}
		Object pstmt = conn.prepare(null, query.toString());
		conn.executePrepared(pstmt, params);

		conn.destroyPrepared(pstmt);
		
		conn.assertResults("select count(*) from foo", br(nr, new Long(paramCount)));
		
		query.append(",(?)");
		params.add(new Short((short) 2));
		try {
			pstmt = conn.prepare(null, query.toString());
		} catch (Throwable t) {
			assertException(t, PESQLStateException.class ,"(1390: HY000) Prepared statement contains too many placeholders");
		}

		conn.disconnect();
	}
	
	@Test
	public void prepstmtTestDBhelper() throws Throwable {
		Properties props = new Properties();
		props.setProperty("useServerPrepStmts", "true");

		PortalDBHelperConnectionResource conn = new PortalDBHelperConnectionResource(props);
		executeTest(conn);
		conn.disconnect();
	}

	private void executeTest(ConnectionResource conn) throws Throwable {
		conn.execute(null, "use prep");

		Object pstmt = conn.prepare(null, "set names 'utf8'");
		List<Object> params = new ArrayList<Object>();
		params.clear();
		conn.executePrepared(pstmt, params);
		conn.destroyPrepared(pstmt);

		pstmt = conn.prepare(null, "insert into foo values (?),(?),(?)");

		params.add(new Integer(2));
		params.add(new Integer(5));
		params.add(null);
		conn.executePrepared(pstmt, params);

		conn.destroyPrepared(pstmt);
		
		pstmt = conn.prepare(null, "insert into bar values (?, ?, ?)");
		conn.executePrepared(pstmt, params);

		conn.destroyPrepared(pstmt);
		

		pstmt = conn.prepare(null, "select * from foo where col1 = ?");

		params.clear();
		params.add(new Integer(2));
		ResourceResponse rr = conn.executePrepared(pstmt, params);
		rr.assertResults("Row value 2", br(nr, 2));
		params.clear();
		params.add(new Integer(5));
		rr = conn.executePrepared(pstmt, params);
		rr.assertResults("Row value 5", br(nr, 5));
		params.clear();
		params.add(new Integer(6));
		rr = conn.executePrepared(pstmt, params);
		rr.assertResults("Row value 6", br());
		conn.destroyPrepared(pstmt);
	}
}
