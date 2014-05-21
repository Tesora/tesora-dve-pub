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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class ShowTest extends SchemaTest {
	private static final String DB_NAME = "show_db";

	private static final ProjectDDL checkDDL = new PEDDL(DB_NAME,
			new StorageGroupDDL("show", 2, "show_grp"), "schema");

	@BeforeClass
	public static void setup() throws Exception {
		projectSetup(checkDDL);
		bootHost = BootstrapHost.startServices(PETest.class);
	}

	protected ProxyConnectionResource conn;
	protected DBHelperConnectionResource dbh;

	@Before
	public void connect() throws Throwable {
		conn = new ProxyConnectionResource();
		checkDDL.create(conn);
		dbh = new DBHelperConnectionResource();
	}

	@After
	public void disconnect() throws Throwable {
		if(conn != null)
			conn.disconnect();
		conn = null;
		if(dbh != null)
			dbh.disconnect();
		dbh = null;
	}

	@Test
	public void testPE952EmptyResultSet() throws Throwable {
		String[][] testValues = {
				// show, column_name, show, show_db, show_like_where, select, select_where
				{ "Function Status", "", "true", "", "false", "false", "false" },
				{ "Triggers", "trigger_name", "true", DB_NAME, "true", "false", "false" },
				//				Should be allowed to select from triggers table
				//				{ "Triggers", "trigger_name", "true", DB_NAME, "false", "true", "any_will_do" },
				{ "Events", "event_name", "true", DB_NAME, "any_will_do", "true", "any_will_do" },
		};

		for (String[] rec : testValues) {
			String name = rec[0];
			String columnName = rec[1];
			boolean doShow = Boolean.parseBoolean(rec[2]);
			String showDb = rec[3];
			String showLikeWhere = rec[4];
			boolean doSelect = Boolean.parseBoolean(rec[5]);
			String selectWhere = rec[6];

			doTest(name, columnName, doShow, showDb, showLikeWhere, doSelect, selectWhere,
					new Object[][] { br(), br(), br(), br(), br() });
		}
	}

	@Test
	public void testPE970ShowEngines() throws Throwable {
		Object[] innodbResult = br(nr, "InnoDB", "DEFAULT",
				"Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES");
		Object[] myisamResult = br(nr, "MyISAM", "YES", "MyISAM storage engine", "NO", "NO", "NO");
		List<Object> results = new ArrayList<Object>(Arrays.asList(innodbResult));
		results.addAll(Arrays.asList(myisamResult));
		Object[] fullResult = results.toArray();

		doTest("Engines", "Engine", true, null, null, true, "MyISAM",
				new Object[][] { fullResult, fullResult, innodbResult, fullResult, myisamResult });
	}

	@Test
	public void testShowCharset() throws Throwable {
		Object[] latin1Result = br(nr, "latin1", "cp1252 West European", new Long(1));
		Object[] asciiResult = br(nr, "ascii", "US ASCII", new Long(1));
		Object[] utf8Result = br(nr, "utf8", "UTF-8 Unicode", new Long(3) ); 
		Object[] utf8mb4Result = br(nr, "utf8mb4", "UTF-8 Unicode", new Long(4) ); 
		List<Object> results = new ArrayList<Object>(Arrays.asList(asciiResult));
		results.addAll(Arrays.asList(latin1Result));
		results.addAll(Arrays.asList(utf8Result));
		results.addAll(Arrays.asList(utf8mb4Result));
		Object[] fullResult = results.toArray();
		
		doTest("Charset", null, true, null, "latin1", false, null, 
				new Object[][] { fullResult, null, latin1Result, null, null });
	}
	
	/**
	 * resultSets is: show, show_with_db, show_with_like, select, select_with_where
	 * 
	 * @param name
	 * @param columnName
	 * @param doShow
	 * @param showDb
	 * @param showLikeWhere
	 * @param doSelect
	 * @param selectWhere
	 * @param resultSets
	 * @throws Throwable
	 */
	private void doTest(String name, String columnName, boolean doShow, String showDb,
			String showLikeWhere, boolean doSelect, String selectWhere,
			Object[][] resultSets) throws Throwable {
		StringBuilder buf;

		if (doShow) {
			buf = new StringBuilder();
			buf.append("SHOW " + name);
			conn.assertResults(buf.toString(), resultSets[0]);

			buf = new StringBuilder();
			buf.append("SHOW " + name.toUpperCase(Locale.ENGLISH));
			conn.assertResults(buf.toString(), resultSets[0]);

			buf = new StringBuilder();
			buf.append("SHOW " + name.toLowerCase(Locale.ENGLISH));
			conn.assertResults(buf.toString(), resultSets[0]);

			if (showDb != null && !showDb.isEmpty()) {
				buf = new StringBuilder();
				buf.append("SHOW " + name + " FROM `" + showDb + "`");
				conn.assertResults(buf.toString(), resultSets[1]);

				buf = new StringBuilder();
				buf.append("SHOW " + name + " FROM " + showDb);
				conn.assertResults(buf.toString(), resultSets[1]);
			}

			if (showLikeWhere != null && !showLikeWhere.isEmpty()) {
				buf = new StringBuilder();
				buf.append("SHOW " + name + " LIKE '%" + showLikeWhere + "%'");
				conn.assertResults(buf.toString(), resultSets[2]);

				// TODO - DAS - I'm disabling this because I don't believe the syntax
				//  this is generating is valid "SHOW EVENTS WHERE 'string'" generates a
				//  warning in native mysql
//				buf = new StringBuilder();
//				buf.append("SHOW " + name + " WHERE '%" + showLikeWhere + "%'");
//				conn.assertResults(buf.toString(), resultSets[2]);
			}
		}

		if (doSelect) {
			buf = new StringBuilder();
			buf.append("SELECT * FROM INFORMATION_SCHEMA." + name);
			conn.assertResults(buf.toString(), resultSets[3]);

			if (selectWhere != null && !selectWhere.isEmpty()) {
				buf = new StringBuilder();
				buf.append("SELECT * FROM INFORMATION_SCHEMA." + name
						+ " WHERE `" + columnName + "` LIKE '%" + selectWhere + "%'");
				conn.assertResults(buf.toString(), resultSets[4]);

				try {
					buf = new StringBuilder();
					buf.append("SELECT * FROM INFORMATION_SCHEMA." + name
							+ " WHERE fred LIKE '%" + selectWhere + "%'");
					conn.execute(buf.toString());
					fail("Bad " + name + " column should throw exception");
				} catch (Exception e) {
					// error expected
				}
			}
		}
	}

	@Test
	public void testShowGenerationSites() throws Throwable {
		Object[] gen0 = {"show_grp",  new Integer(0), "show0"};
		Object[] gen1 = {"show_grp",  new Integer(0), "show1"};
		Object[] sysgrp = {"SystemGroup",  new Integer(0), "SystemSite"};
		Object[][] fullResult = {
				{gen0, gen1, sysgrp},
				null,
				{sysgrp}
		};
		
		doTest("generation sites", null, true, null, "SystemGroup", false, null, fullResult); 

		Object[][] fullResult2 = {{gen1}};
		doTest("generation sites where Site='show1'", null, true, null, null, false, null, fullResult2); 
	}
	
	@Test
	public void testTemplateOnDatabase() throws Throwable {
		final Object[][] fullResult = {
				br(nr, DB_NAME, null, TemplateMode.OPTIONAL.toString())
		};

		doTest("template on database " + DB_NAME, null, true, null, null, false, null, fullResult);
		doTest("template on schema " + DB_NAME, null, true, null, null, false, null, fullResult);
	}

}
