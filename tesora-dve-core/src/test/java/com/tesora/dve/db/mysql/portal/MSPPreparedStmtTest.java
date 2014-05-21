// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

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

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.TestDataGenerator;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.sql.SchemaMirrorTest;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.DBHelperConnectionResource;
import com.tesora.dve.sql.util.MirrorFun;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PortalDBHelperConnectionResource;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class MSPPreparedStmtTest extends SchemaMirrorTest {
	private static ColumnSet testColumns = new ColumnSet();
	private static UserTable testTable = new UserTable();
	static {
		testTable.setName("pstest");
		testColumns.addColumn("colint", 11, "int", Types.INTEGER);
		testColumns.addColumn("colbit", 1, "tinyint", Types.BOOLEAN);
		testColumns.addColumn("coltinyint", 4, "tinyint", Types.TINYINT);
		testColumns.addColumn("colsmallint", 6, "smallint", Types.SMALLINT);
		testColumns.addColumn("colmedint", 9, "mediumint", Types.SMALLINT);
		testColumns.addColumn("colbigint", 21, "bigint", Types.BIGINT);
		testColumns.addColumn("colvarchar", 10, "varchar", Types.VARCHAR);
		testColumns.addColumn("coldouble", 10, "double", Types.DOUBLE, 10, 5);
		testColumns.addColumn("colfloat", 10, "float", Types.FLOAT, 5, 2);
		testColumns.addColumn("coldecimal", 10, "decimal", Types.DECIMAL, 10, 5);
		testColumns.addColumn("coldate", 8, "date", Types.DATE);
		testColumns.addColumn("coldatetime", 8, "datetime", Types.TIMESTAMP);
		testColumns.addColumn("coltime", 8, "time", Types.TIME);
		try {
			testTable.addColumnMetadataList(testColumns.getColumnList());
		} catch (PEException e) {
			throw new PECodingException(e);
		}
	}

	private static TestDataGenerator tdg = new TestDataGenerator(testColumns);

	private static Properties getUrlOptions() {
		Properties props = new Properties();
		props.setProperty("useServerPrepStmts","true");
		props.setProperty("emulateUnsupportedPstmts","false");
		return props;
	}
	
	private static final ProjectDDL testDDL =
			new PEDDL("checkdb",
					new StorageGroupDDL("check",1,"checkg"),
					"schema");
	private static final NativeDDL nativeDDL =
			new NativeDDL("nativedb");

	@Override
	protected ProjectDDL getSingleDDL() {
		return testDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@BeforeClass
	public static void setup() throws Throwable {
		setup(null, testDDL, nativeDDL, getPopulate());
		tdg.generateRow(0);
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
				if (mr == null) return null;

				return mr.getConnection().execute(testTable.getCreateTableStmt());
			}
		});

		return out;
	}

	@Override
	protected ConnectionResource createConnection(ProjectDDL p) throws Throwable {
		if (p == getNativeDDL()) 
			return new DBHelperConnectionResource(getUrlOptions()); 
		else if (p == getSingleDDL()) 
			return new PortalDBHelperConnectionResource(getUrlOptions());

		throw new PEException("Unsupported ProjectDDL type " + p.getClass());
	}
	
	@Test
	public void insertTest() throws Throwable {
		List<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add( new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				
				Object which = mr.getConnection().prepare(null, getInsertStmt(testTable));

				return mr.getConnection().executePrepared(which,
							// tdg.generateRow(0 /* nullCount */)
							tdg.getGeneratedRow(0)
							);
			}
			
		});
		
		runTest(out);
	}

	@Test
	public void oneParamSelectTest() throws Throwable {
		List<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add( new MirrorFun(true, false) {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;

				Object select = mr.getConnection().prepare(null, "select * from pstest where colsmallint=?");
				List<Object> params = new ArrayList<Object>();
				params.add(tdg.getColumnValue(0, "colsmallint"));
				mr.getConnection().executePrepared(select, params);
				
				// we are going to execute twice - just to ensure that works
				return mr.getConnection().executePrepared(select, params);
			}
		});
		
		runTest(out);
	}

	@Test
	public void twoParamSelectTest() throws Throwable {
		List<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add( new MirrorFun(true, false) {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;

				Object select = mr.getConnection().prepare(null, "select * from pstest where colsmallint=? and colvarchar=?");
				List<Object> params = new ArrayList<Object>();
				params.add(tdg.getColumnValue(0, "colsmallint"));
				params.add(tdg.getColumnValue(0, "colvarchar"));
				return mr.getConnection().executePrepared(select, params);
			}
		});
		
		runTest(out);
	}

	@Test
	public void resultSetTypesTest() throws Throwable {
		List<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new StatementMirrorProc("select * from pstest"));
		
		runTest(out);
	}
	
	private String getInsertStmt(UserTable ut) {
		StringBuffer sb = new StringBuffer().append("insert into ");
		sb.append(ut.getName()).append(" values (");
		
		for (int i = 1; i <= ut.getUserColumns().size(); i++) {
			sb.append("?");
			if ( i == ut.getUserColumns().size() )
				sb.append(")");
			else
				sb.append(",");
		}

		return sb.toString();
	}

}
