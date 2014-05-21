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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class TableMaintenanceTest extends SchemaTest {
	private static String sitePrefix = "check";
	private static int siteCount = 3;
	private static final StorageGroupDDL storageGroup =
			new StorageGroupDDL(sitePrefix, siteCount, "checkg");

	private static final StorageGroupDDL otherStorageGroup =
			new StorageGroupDDL("check1", 1, "checkg1");

	private static String dbName = "checkdb";
	private static final ProjectDDL testDDL =
			new PEDDL(dbName, storageGroup, "schema");

	private static String otherDbName = "otherdb";
	private static final ProjectDDL otherDDL =
			new PEDDL(otherDbName, otherStorageGroup, "database");

	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(testDDL, otherDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);
		ConnectionResource pcr = new PortalDBHelperConnectionResource();
		testDDL.create(pcr);
		otherDDL.create(pcr);
		pcr.disconnect();
		pcr = null;
	}

	protected ConnectionResource conn;

	@Before
	public void before() throws Throwable {
		conn = new PortalDBHelperConnectionResource();
	}

	@After
	public void after() throws Throwable {
		if (conn != null) {
			conn.disconnect();
			conn = null;
		}
	}
	
	@AfterClass
	public static void teardownAfter() throws Throwable {
		ConnectionResource pcr = new PortalDBHelperConnectionResource();
		testDDL.destroy(pcr);
		otherDDL.destroy(pcr);
		pcr.disconnect();
		pcr = null;
	}

	@Test
	public void testSyntax() throws Throwable {
		conn.execute("use `" + dbName + "`");
		conn.execute("create table `a` (`id` int, `mask` varchar(50), primary key (`id`)) ");
		conn.execute("create table `b` (`id` int, `mask` varchar(50), primary key (`id`)) ");
		conn.execute("use `" + otherDbName + "`");
		conn.execute("create table `c` (`id` int, `mask` varchar(50), primary key (`id`)) ");
		conn.execute("use `" + dbName + "`");

		final String otherPersistGroupTable = otherDbName + ".c";
		final String OPERATION_CHECK = "CHECK";
		final String OPERATION_ANALYZE = "ANALYZE";
		final String OPERATION_OPTIMIZE = "OPTIMIZE";
		String[] operations = new String[] {OPERATION_CHECK, OPERATION_ANALYZE, OPERATION_OPTIMIZE};
		String[] tables =  new String[] {"`a`", "`" + dbName + "`.`b`", otherPersistGroupTable};
		String[] options = new String[] {"", "NO_WRITE_TO_BINLOG", "LOCAL" };
		
		for (String operation : operations) {
			for (String option : options) {
				if (operation.equals(OPERATION_CHECK) && !option.isEmpty()) {
					continue;
				}
				for (int i=0; i<tables.length; i++) {
					StringBuilder tableCSV = new StringBuilder();
					List<ResultRow> results = new ArrayList<ResultRow>();
					for (int j=0; j<=i; j++) {
						if (j>0) {
							tableCSV.append(", ");
						}
						tableCSV.append(tables[j]);
						for (int count = 0; count < siteCount; count++) {
							ResultRow rr = new ResultRow();
							String tableName = tables[j].replace(dbName, "").replace("`", "").replace(".", "");
							String qualifiedTableName = sitePrefix + count + "_" + dbName + "." + tableName;
							rr.addResultColumn(qualifiedTableName);
							rr.addResultColumn(operation.toLowerCase(Locale.US));
							rr.addResultColumn("status");
							rr.addResultColumn("OK");
							results.add(rr);
							if (operation.equals(OPERATION_OPTIMIZE)) {
								rr = new ResultRow();
								rr.addResultColumn(qualifiedTableName);
								rr.addResultColumn(operation.toLowerCase(Locale.US));
								rr.addResultColumn("note");
								rr.addResultColumn("Table does not support optimize, doing recreate + analyze instead");
								results.add(rr);
							}
						}
					}
					boolean shouldFail = tableCSV.toString().contains(otherPersistGroupTable);
					String sql = operation + " " + option + " TABLE " + tableCSV;
					try {
						ResourceResponse response = conn.execute(sql);
						response.assertResultsEqualUnordered(sql, results, response.getResults(),
								response.getColumnCheckers());
						if (shouldFail) {
							fail("SQL: '" + sql + "' should fail, due to multiple Persistent Groups");
						}
					} catch (Exception e) {
						if (shouldFail) {
							assertException(e, SQLException.class, "SchemaException: "
									+ "Table 'c' in maintenance command is not in Persistent Group 'checkg'");
						}
						else {
							failWithStackTrace(e);
						}
					}
				}
			}
		}
	}

}
