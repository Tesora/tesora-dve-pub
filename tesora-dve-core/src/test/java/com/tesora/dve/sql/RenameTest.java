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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.errmap.MySQLErrors;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.ddl.RenameTableStatement;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class RenameTest extends SchemaMirrorTest {
	private static final int SITES = 5;

	private static final ProjectDDL sysDDL = new PEDDL("sysdb", new StorageGroupDDL("sys", SITES, "sysg"), "schema");
	static final NativeDDL nativeDDL = new NativeDDL("cdb");
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL, null, nativeDDL, getSchema());
	}

	static final String[] tabNames = new String[] { "B", "S", "A", "R" }; 
	static final String[] distVects = new String[] { 
		"broadcast distribute",
		"static distribute on (`id`)",
		"random distribute",
		"range distribute on (`id`) using "
	};
	private static final String tabBody = " `id` int unsigned auto_increment, `junk` varchar(24), primary key (`id`)";
	
	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				boolean ext = !nativeDDL.equals(mr.getDDL());
				// declare the tables
				ResourceResponse rr = null;
				if (ext) 
					// declare the range
					mr.getConnection().execute("create range open" + mr.getDDL().getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName());
				List<String> actTabs = new ArrayList<String>();
				actTabs.addAll(Arrays.asList(tabNames));
				// actTabs.add("T");
				for(int i = 0; i < actTabs.size(); i++) {
					String tn = actTabs.get(i);
					StringBuilder buf = new StringBuilder();
					buf.append("create table `").append(tn).append("` ( ").append(tabBody).append(" ) ");
					if (ext && i < 4) {
						buf.append(distVects[i]);
						if ("R".equals(tabNames[i]))
							buf.append(" open").append(mr.getDDL().getDatabaseName());
					}
					rr = mr.getConnection().execute(buf.toString());
				}
				return rr;
			}
		});
		return out;
	}	
	
	@Test
	public void testPE741_RenameActionsReduction() throws Throwable {
		final Name a = new UnqualifiedName("A");
		final Name b = new UnqualifiedName("B");
		final Name c = new UnqualifiedName("C");
		final Name d = new UnqualifiedName("D");
		final Name e = new UnqualifiedName("E");
		final Name f = new UnqualifiedName("F");

		final RenameTableStatement.IntermediateSchema schema = new RenameTableStatement.IntermediateSchema();
		final Map<Name, Name> expected = new LinkedHashMap<Name, Name>();

		/* Snake */
		schema.addRenameAction(a, b);
		schema.addRenameAction(b, c);
		schema.addRenameAction(c, d);

		expected.put(a, d);

		assertRenameActions(expected, schema.getRenameActions());

		expected.clear();
		schema.clear();

		/* Swap */
		schema.addRenameAction(a, b);
		schema.addRenameAction(c, a);
		schema.addRenameAction(b, c);

		expected.put(c, a);
		expected.put(a, c);
		
		assertRenameActions(expected, schema.getRenameActions());

		expected.clear();
		schema.clear();

		/* Backup */
		schema.addRenameAction(a, b);
		schema.addRenameAction(c, a);

		expected.put(a, b);
		expected.put(c, a);

		assertRenameActions(expected, schema.getRenameActions());

		expected.clear();
		schema.clear();

		/* Complex */
		schema.addRenameAction(a, b);
		schema.addRenameAction(c, a);
		schema.addRenameAction(d, e);
		schema.addRenameAction(b, c);
		schema.addRenameAction(a, f);
		schema.addRenameAction(f, d);

		expected.put(d, e);
		expected.put(a, c);
		expected.put(c, d);

		assertRenameActions(expected, schema.getRenameActions());
	}

	@Test
	public void testPE741_RenameDatabase_Deprecated() throws Throwable {
		try (final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource()) {
			final String dbName = sysDDL.getDatabaseName();
			connection.execute("USE " + dbName);

			final String expectedErrorMessage = "Internal error: This syntax is not supported as it has been deprecated.";

			new ExpectedExceptionTester() {
				@Override
				public void test() throws Throwable {
					connection.execute("RENAME DATABASE A TO B");
				}
			}.assertException(SQLException.class, expectedErrorMessage);

			new ExpectedExceptionTester() {
				@Override
				public void test() throws Throwable {
					connection.execute("RENAME SCHEMA A TO B");
				}
			}.assertException(SQLException.class, expectedErrorMessage);
		}
	}

	@Test
	public void testPE741_RenameTable_Validations() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		final String dbName = sysDDL.getDatabaseName();
		connection.execute("USE " + dbName);
		
		final Name t1 = getTableName(null, "pe741_1");
		final Name t2 = getTableName(null, "pe741_2");

		createTables(connection, t1, t2);

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				connection.execute("RENAME TABLE pe741_1 TO pe741_2");
			}
		}.assertException(SQLException.class, "Internal error: Table 'sysdb.pe741_2' already exists."); 
		
		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				connection.execute("RENAME TABLE pe741_3 TO pe741_4");
			}
		}.assertException(SQLException.class, "Internal error: No such table '" + dbName + ".pe741_3'.");
		
		try {
			connection.execute("CREATE DATABASE IF NOT EXISTS pe741_temp USING TEMPLATE OPTIONAL");
			new ExpectedExceptionTester() {
				@Override
				public void test() throws Throwable {
					connection.execute("RENAME TABLE pe741_temp.pe741_3 TO pe741_4");
				}
			}.assertException(SQLException.class, "Internal error: Moving tables between databases and persistent groups is not allowed.");
		} finally {
			connection.execute("DROP DATABASE IF EXISTS pe741_temp");
		}

		new ExpectedSqlErrorTester() {
			@Override
			public void test() throws Throwable {
				connection.execute("RENAME TABLE oops.pe741_3 TO pe741_4");
			}
		}.assertError(SQLException.class, MySQLErrors.unknownDatabaseFormatter, "Unknown database 'oops'");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				connection.execute("RENAME TABLE pe741_1 TO pe741_3, pe741_2 TO pe741_1, pe741_1 TO pe741_3, pe741_3 TO pe741_2");
			}
		}.assertException(SQLException.class, "Internal error: Table '" + dbName + ".pe741_3' already exists.");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				connection.execute("RENAME TABLE pe741_1 TO pe741_3, pe741_3 TO pe741_2");
			}
		}.assertException(SQLException.class, "Internal error: Table '" + dbName + ".pe741_2' already exists.");

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Throwable {
				connection.execute("RENAME TABLE pe741_1 TO pe741_4, pe741_2 TO pe741_1, pe741_4 TO pe741_3, pe741_4 TO pe741_2");
			}
		}.assertException(SQLException.class, "Internal error: No such table '" + dbName + ".pe741_4'.");
	}

	@Test
	public void testPE741_RenameTable_Simple() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		connection.execute("USE " + sysDDL.getDatabaseName());

		final Name t1 = getTableName(null, "pe741_Simple_1");
		final Name t2 = getTableName(null, "pe741_Simple_2");
		final Name t3 = getTableName(null, "pe741_Simple_A");
		final Name t4 = getTableName(null, "pe741_Simple_B");

		createTables(connection, t1, t2);

		final ListOfPairs<Name, Name> sourceTargetNames = new ListOfPairs<Name, Name>();
		sourceTargetNames.add(new Pair<Name, Name>(t1, t3));
		sourceTargetNames.add(new Pair<Name, Name>(t2, t4));

		testRename(connection, sourceTargetNames);
	}

	@Test
	public void testPE741_RenameTable_Simple_MultiDb() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		final String dbName = sysDDL.getDatabaseName();

		final Name t1 = getTableName(dbName, "pe741_Simple_MultiDb_1");
		final Name t2 = getTableName(dbName, "pe741_Simple_MultiDb_2");
		final Name t3 = getTableName(dbName, "pe741_Simple_MultiDb_A");
		final Name t4 = getTableName(dbName, "pe741_Simple_MultiDb_B");

		createTables(connection, t1, t2);

		final ListOfPairs<Name, Name> sourceTargetNames = new ListOfPairs<Name, Name>();
		sourceTargetNames.add(new Pair<Name, Name>(t1, t3));
		sourceTargetNames.add(new Pair<Name, Name>(t2, t4));

		testRename(connection, sourceTargetNames);
	}

	@Test
	public void testPE741_RenameTable_Backup() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		connection.execute("USE " + sysDDL.getDatabaseName());

		final Name t1 = getTableName(null, "pe741_Backup_old");
		final Name t2 = getTableName(null, "pe741_Backup_new");
		final Name t3 = getTableName(null, "pe741_Backup_backup");

		createTables(connection, t1, t2);

		final ListOfPairs<Name, Name> sourceTargetNames = new ListOfPairs<Name, Name>();
		sourceTargetNames.add(new Pair<Name, Name>(t1, t3));
		sourceTargetNames.add(new Pair<Name, Name>(t2, t1));

		testRename(connection, sourceTargetNames);
	}

	@Test
	public void testPE741_RenameTable_Swap() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		connection.execute("USE " + sysDDL.getDatabaseName());

		final Name t1 = getTableName(null, "pe741_Swap_A");
		final Name t2 = getTableName(null, "pe741_Swap_B");
		final Name t3 = getTableName(null, "pe741_Swap_TMP");

		createTables(connection, t1, t2);

		final ListOfPairs<Name, Name> sourceTargetNames = new ListOfPairs<Name, Name>();
		sourceTargetNames.add(new Pair<Name, Name>(t1, t3));
		sourceTargetNames.add(new Pair<Name, Name>(t2, t1));
		sourceTargetNames.add(new Pair<Name, Name>(t3, t2));

		testRename(connection, sourceTargetNames);
	}

	@Test
	public void testPE741_RenameTable_Snake() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		connection.execute("USE " + sysDDL.getDatabaseName());

		final Name t1 = getTableName(null, "pe741_Snake_A");
		final Name t2 = getTableName(null, "pe741_Snake_B");
		final Name t3 = getTableName(null, "pe741_Snake_C");
		final Name t4 = getTableName(null, "pe741_Snake_D");

		createTables(connection, t1);

		final ListOfPairs<Name, Name> sourceTargetNames = new ListOfPairs<Name, Name>();
		sourceTargetNames.add(new Pair<Name, Name>(t1, t2));
		sourceTargetNames.add(new Pair<Name, Name>(t2, t3));
		sourceTargetNames.add(new Pair<Name, Name>(t3, t4));

		testRename(connection, sourceTargetNames);
	}

	@Test
	public void testPE741_RenameTable_Complex() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		connection.execute("USE " + sysDDL.getDatabaseName());

		final Name t1 = getTableName(null, "pe741_Complex_A");
		final Name t2 = getTableName(null, "pe741_Complex_B");
		final Name t3 = getTableName(null, "pe741_Complex_C");
		final Name t4 = getTableName(null, "pe741_Complex_D");
		final Name t5 = getTableName(null, "pe741_Complex_E");
		final Name t6 = getTableName(null, "pe741_Complex_F");

		createTables(connection, t1, t3, t4);

		final ListOfPairs<Name, Name> sourceTargetNames = new ListOfPairs<Name, Name>();
		sourceTargetNames.add(new Pair<Name, Name>(t1, t2));
		sourceTargetNames.add(new Pair<Name, Name>(t3, t1));
		sourceTargetNames.add(new Pair<Name, Name>(t4, t5));
		sourceTargetNames.add(new Pair<Name, Name>(t2, t3));
		sourceTargetNames.add(new Pair<Name, Name>(t1, t6));
		sourceTargetNames.add(new Pair<Name, Name>(t6, t4));

		testRename(connection, sourceTargetNames);
	}

	private void createTables(final PortalDBHelperConnectionResource connection, final Name... tableNames) throws Throwable {
		for (final Name tableName : tableNames) {
			createTable(connection, tableName);
		}
	}

	private void createTable(final PortalDBHelperConnectionResource connection, final Name tableName) throws Throwable {
		final String sqlName = tableName.getSQL();
		connection.execute("DROP TABLE IF EXISTS " + sqlName);
		connection.execute("CREATE TABLE " + sqlName + " (`name` TINYTEXT NOT NULL) ENGINE=InnoDB;");
		connection.execute("INSERT INTO " + sqlName + " VALUES ('" + tableName + "')");

		validateTable(connection, tableName, tableName.get());
	}

	private void testRename(final PortalDBHelperConnectionResource connection, final ListOfPairs<Name, Name> sourceTargetNames) throws Throwable {
		final RenameTableStatement.IntermediateSchema schema = new RenameTableStatement.IntermediateSchema();
		final StringBuilder renameStatement = new StringBuilder("RENAME TABLE ");
		for (final Pair<Name, Name> namePair : sourceTargetNames) {
			final Name oldName = namePair.getFirst();
			final Name newName = namePair.getSecond();
			renameStatement.append(oldName.getSQL()).append(" TO ").append(newName.getSQL()).append(",");
			schema.addRenameAction(oldName, newName);
		}
		renameStatement.deleteCharAt(renameStatement.length() - 1);

		connection.execute(renameStatement.toString());

		validateRenameResults(connection, schema.getRenameActions());

	}

	private void validateRenameResults(final PortalDBHelperConnectionResource connection, final Map<Name, Name> namePairs) throws Throwable {
		for (final Map.Entry<Name, Name> namePair : namePairs.entrySet()) {
			final Name oldName = namePair.getValue();
			final Name newName = namePair.getKey();
			validateTable(connection, newName, oldName.get());
		}
	}

	private void validateTable(final PortalDBHelperConnectionResource connection, final Name tableName, final String nameFieldValue) throws Throwable {
		final String unqualifiedTableName = tableName.getUnqualified().get();
		final String dbName = (tableName.isQualified()) ? ((QualifiedName) tableName).getNamespace().getSQL() : null;
		connection.assertResults("SELECT name FROM " + tableName.getSQL(), br(nr, nameFieldValue));

		final String schemaCheck = "SHOW TABLES" + ((dbName != null) ? " IN " + dbName : "") + " LIKE '" + unqualifiedTableName + "'";
		connection.assertResults(schemaCheck, br(nr, unqualifiedTableName));
	}

	private Name getTableName(final String dbName, final String tableName) {
		if (dbName != null) {
			return new QualifiedName(new UnqualifiedName(dbName), new UnqualifiedName(tableName));
		}

		return new UnqualifiedName(tableName);
	}

	/**
	 * NOTE: Actual value pairs are stored in opposite order.
	 */
	private void assertRenameActions(final Map<Name, Name> expected, final Map<Name, Name> actual) {
		assertEquals(expected.size(), actual.size());

		final Iterator<Map.Entry<Name, Name>> expectedValues = expected.entrySet().iterator();
		final Iterator<Map.Entry<Name, Name>> actualValues = actual.entrySet().iterator();
		while (expectedValues.hasNext()) {
			final Map.Entry<Name, Name> expectedNamePair = expectedValues.next();
			final Map.Entry<Name, Name> actualNamePair = actualValues.next();

			final String expectedVector = expectedNamePair.getKey() + " -> " + expectedNamePair.getValue();
			final String actualVector = actualNamePair.getValue() + " -> " + actualNamePair.getKey();

			assertEquals(expectedVector, actualVector);
		}
	}

}