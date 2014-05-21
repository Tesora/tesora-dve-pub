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

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;

public class TruncateTest extends SchemaMirrorTest {

	public static String buildTableColumnInsert(final String table, final String column, final List<?> values) {
		final StringBuilder insertStatement = new StringBuilder("insert into ");
		insertStatement.append(table);
		insertStatement.append(" (").append(column).append(")");
		insertStatement.append(" values ");
		for (final Object value : values) {
			insertStatement.append("(").append(value).append(")").append(",");
		}
		insertStatement.deleteCharAt(insertStatement.length() - 1);

		return insertStatement.toString();
	}

	public static void testTableTruncate(final ConnectionResource connection, final String db, final String table, final long initialRowCount) throws Throwable {
		final String countStatement = "select count(*) from " + table;
		connection.execute("use " + db);
		connection.assertResults(countStatement, br(nr, initialRowCount));
		final ResourceResponse response = connection.execute("truncate table " + table);
		connection.assertResults(countStatement, br(nr, 0l));
		assertEquals(0l, response.getNumRowsAffected());
	}

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
		setup(sysDDL, null, nativeDDL, Collections.<MirrorTest> emptyList());
	}

	@Test
	public void testNonAi() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		try {
			final String dbName = sysDDL.getDatabaseName();
			connection.execute("use " + dbName);

			connection.execute("create table a (id INT not null)");
			connection.execute(buildTableColumnInsert("a", "id", Arrays.asList(1, 2, 3)));
			testTableTruncate(connection, dbName, "a", 3);
		} finally {
			connection.close();
		}
	}

	@Test
	public void testAi() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		try {
			final String dbName = sysDDL.getDatabaseName();
			connection.execute("use " + dbName);

			connection.execute("create table b (id INT not null auto_increment)");
			connection.execute(buildTableColumnInsert("b", "id", Arrays.asList(null, null, null)));
			testTableTruncate(connection, dbName, "b", 3);
			connection.execute(buildTableColumnInsert("b", "id", Arrays.asList(null, null, null)));
			connection.assertResults("select * from b order by id", br(nr, 1, nr, 2, nr, 3));
		} finally {
			connection.close();
		}
	}

	@Test
	public void testTruncateView() throws Throwable {
		final PortalDBHelperConnectionResource connection = new PortalDBHelperConnectionResource();
		try {
			final String dbName = sysDDL.getDatabaseName();
			connection.execute("use " + dbName);

			connection.execute("create table t (id INT not null auto_increment)");
			connection.execute(buildTableColumnInsert("t", "id", Arrays.asList(null, null, null)));
			connection.execute("create view v as select * from t");

			new ExpectedExceptionTester() {
				@Override
				public void test() throws Throwable {
					connection.execute("truncate table v");
				}
			}.assertException(SQLException.class, "SchemaException: 'v' is not a base table");

			testTableTruncate(connection, dbName, "t", 3);
		} finally {
			connection.close();
		}
	}

}
