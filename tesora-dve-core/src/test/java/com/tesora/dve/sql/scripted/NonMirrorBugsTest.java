package com.tesora.dve.sql.scripted;

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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class NonMirrorBugsTest extends SchemaTest {

	private static final ProjectDDL pe504DDL =
			new PEDDL("dtdb",
					new StorageGroupDDL("dtsite",13,"dtgroup"),
					"database");
	
	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(pe504DDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		setTemplateModeOptional();
	}

	ConnectionResource conn;
	
	@Before
	public void connect() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
	}
	
	@After
	public void disconnect() throws Throwable {
		if(pe504DDL != null)
			pe504DDL.destroy(conn);

		if(conn != null)
			conn.disconnect();
		conn = null;
	}
	
	@Test
	public void testPE504() throws Throwable {
		String jdbc = TestCatalogHelper.getInstance().getCatalogUrl();
		String actualJdbc = "jdbc:mysql://localhost:3307";
		List<String> stmts = importTest(this.getClass(),"pe504.sql",actualJdbc,jdbc);
		runTest(conn,stmts);
	}
	
	@Ignore
	@Test
	public void testPE505() throws Throwable {
		String jdbc = TestCatalogHelper.getInstance().getCatalogUrl();
		String actualJdbc = "jdbc:mysql://localhost:3307";
		List<String> stmts = importTest(this.getClass(),"pe505.sql",actualJdbc,jdbc);
		runTest(conn,stmts);
	}
	
	@Test
	public void testPE506() throws Throwable {
		String jdbc = TestCatalogHelper.getInstance().getCatalogUrl();
		String actualJdbc = "jdbc:mysql://localhost:3307";
		List<String> stmts = importTest(this.getClass(),"pe506.sql",actualJdbc,jdbc);
		runTest(conn,stmts);
	}
	
	@Test
	public void testPE494() throws Throwable {
		String jdbc = TestCatalogHelper.getInstance().getCatalogUrl();
		String actualJdbc = "jdbc:mysql://localhost:3307";
		List<String> stmts = importTest(this.getClass(),"pe494.sql",actualJdbc,jdbc);
		runTest(conn,stmts);
	}
	
	@Test
	public void testPE520() throws Throwable {
		conn.execute("alter dve set plan_cache_limit = 0");
		conn.execute("alter dve set cache_limit = 0");
		String jdbc = TestCatalogHelper.getInstance().getCatalogUrl();
		String actualJdbc = "jdbc:mysql://localhost:3305";
		List<String> stmts = importTest(this.getClass(), "pe520.sql", actualJdbc, jdbc);
		Map<Integer, String> locations = new LinkedHashMap<Integer, String>();
		// try {
		for (String s : stmts)
			try {
				if (s.startsWith("SELECT"))
					verifyLocations(locations, conn, s);
				else
					conn.execute(s);
			} catch (PEException e) {
				// e.printStackTrace();
				throw new Throwable("Unable to execute '" + s + "'", e);
			}
		// } finally {
		// 	System.out.println("dvix/site");
		// 	for(Map.Entry<Integer,String> me : locations.entrySet()) {
		// 		System.out.println(me.getKey() + "/" + me.getValue());
		// 	}
		// }
	}	
	
	// {table-name,dvix,site-name}
	private void verifyLocations(Map<Integer,String> locs, ConnectionResource conn1, String stmt) throws Throwable {
		ResourceResponse rr = conn1.fetch(stmt);
		List<ResultRow> results = rr.getResults();
		for(ResultRow row : results) {
			// the output is @dve_sitename, dvix, rix, table-name
			String table = (String) row.getResultColumn(4).getColumnValue();
			Integer dvix = (Integer) row.getResultColumn(2).getColumnValue();
			String site = (String) row.getResultColumn(1).getColumnValue();
			assertNotNull("@dve_sitename returned null for " + dvix, site);
			String existing = locs.get(dvix);
			if (existing != null) {
				if (!existing.equals(site)) {
					fail("For table " + table + ", dvix " + dvix + ", previous site was " + existing + ", now it is " + site);
				}
			} else {
				locs.put(dvix, site);
			}
		}
	}
	
	
	public static void runTest(ConnectionResource conn, List<String> stmts) throws Throwable {
		for(String s : stmts) try {
			conn.execute(s);
		} catch (PEException e) {
			e.printStackTrace();
			throw new Throwable("Unable to execute '" + s + "'", e);
		}
	}
	
	public static List<String> importTest(Class<?> relativeTo, String srcFile, String givenJDBCUrl, String actualJDBCUrl) throws Throwable {
		ArrayList<String> out = new ArrayList<String>();
		final String comment = "--";
		InputStream is = relativeTo.getResourceAsStream(srcFile);
		String creds = "user='" + TestCatalogHelper.getInstance().getCatalogUser() + "' password='" + TestCatalogHelper.getInstance().getCatalogPassword() + "'";
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			StringBuffer buf = null;
			String line = null;
			do {
				line = reader.readLine();
				if (line != null) {

					line = line.trim();
					if (line.isEmpty() || line.startsWith(comment)) {
						continue;
					}
					if (line.indexOf(givenJDBCUrl) > -1) {
						line = line.replace(givenJDBCUrl, actualJDBCUrl);
						if (line.endsWith(";")) {
							line = line.replace(";", creds);
							line += ";";
						} else {
							line += creds;
						}
					}
					if (buf == null)
						buf = new StringBuffer();
					buf.append(line);
					if (line.endsWith(";")) {
						out.add(buf.toString());
						buf = null;
					}
				}
			} while (line != null);
			if (buf != null) {
				out.add(buf.toString());
				buf = null;
			}
		} finally {
			is.close();
		}
		return out;
	}
}
