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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.sql.parser.ParserInvoker.LineTag;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ColumnChecker;
import com.tesora.dve.sql.util.ConnectionResource;
import com.tesora.dve.sql.util.MirrorApply;
import com.tesora.dve.sql.util.MirrorFun;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.TestResource;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.variable.SchemaVariableConstants;

/**
 * Class for tests requiring a complete DVE system up and running, not just the engine. 
 * 
 */
public class SchemaTest extends PETest {
	
	private static boolean noisy = Boolean.valueOf(System.getProperty("parser.debug")).booleanValue();

	protected SchemaTest() {
		super();
		SSConnectionProxy.setOperatingContext(this.getClass().getSimpleName());
	}

	public static boolean isNoisy() {
		return noisy;
	}
	
	protected static boolean setNoisy(boolean v) {
		boolean o = noisy;
		noisy = v;
		return o;
	}
	
	public static void echo(String what) {
		if (noisy)
			System.out.println(what);
	}
	
	protected static DBVersion backingDBVersion = null;
	
	protected static DBVersion getDBVersion() {
		if (backingDBVersion == null) {
			DBHelper helper = null;
			ResultSet rs = null;
			String versionString = null;
			try {
				helper = PETest.buildHelper();
				helper.executeQuery("select @@version");
				rs = helper.getResultSet();
				rs.next();
				versionString = rs.getString(1);
				rs.close();
			} catch (Throwable t) {
				throw new RuntimeException("Unable to determine backing database version",t);
			} finally {
				if (rs != null) try {
					rs.close();
				} catch (Throwable t) {
					// ignore
				}
				if (helper != null) try {
					helper.disconnect();
				} catch (Throwable t) {
					// ignore
				}
			}
			DBVersion maybe = null;
			if (versionString.startsWith("5.1"))
				maybe = DBVersion.MYSQL_51;
			else
				maybe = DBVersion.MYSQL_55;
			synchronized(SchemaTest.class) {
				if (backingDBVersion == null)
					backingDBVersion = maybe;
			}
		}
		return backingDBVersion;
	}
	
	protected void assertColumnIs(SchemaContext sc, ExpressionNode e, PETable tab, String name) {
		PEColumn c = tab.lookup(sc,name);
		ColumnInstance ci = (ColumnInstance)e;
		assertTrue(ci.getColumn().equals(c));
	}
	
	protected void assertColumnsAre(SchemaContext sc, List<ExpressionNode> cols, PETable tab, String[] names) {
		assertEquals(cols.size(), names.length);
		for(int i = 0; i < names.length; i++)
			assertColumnIs(sc, cols.get(i), tab, names[i]);
	}
	
	protected void assertValueIs(SchemaContext sc, ExpressionNode e, Object v) {
		LiteralExpression l = (LiteralExpression)e;
		if (v == null)
			assertTrue(l.isNullLiteral());
		else
			assertEquals(l.getValue(sc), v);
	}
	
	protected void assertValuesAre(SchemaContext sc, List<ExpressionNode> cols, Object[] vs) {
		assertEquals(cols.size(), vs.length);
		for(int i = 0; i < vs.length; i++)
			assertValueIs(sc, cols.get(i), vs[i]);
	}

	protected static void assertInstanceOf(Object o, Class<?> c) {
		if (!c.isInstance(o))
			fail("Expected object of type " + c.getName() + " but found type " + o.getClass().getName() + " instead");
	}
		
	protected static final Object ignore = new Object();
	
	protected static void assertRowEquals(String cntxt, ResultRow rr, Object[] values, List<ColumnChecker> checkers) {
		List<ResultColumn> row = rr.getRow();
		assertEquals(row.size(),values.length);
		assertEquals(row.size(), checkers.size());
		for(int i = 0; i < values.length; i++) {
			if (values[i] == getIgnore())
				continue;
			String diffs = checkers.get(i).isEqual(cntxt, values[i], row.get(i).getColumnValue(), false);
			if (diffs != null) fail(diffs);
		}
	}
	
	protected static void assertRowsEqual(String cntxt, List<ResultRow> rows, Object[] values, List<ColumnChecker> checkers) {
		assertEquals(rows.size(), values.length);
		for(int i = 0; i < values.length; i++) {
			assertRowEquals(cntxt, rows.get(i), (Object[])values[i], checkers);
		}
	}
	
	protected static final Object nr = new Object();
	protected static final Object I_ONE = new Integer(1);
	protected static final Object I_ZERO = new Integer(0);
	
	protected static Object[] br(Object ...args) {
		ArrayList<Object> rows = new ArrayList<Object>();
		ArrayList<Object> buf = null;
		for(int i = 0; i < args.length; i++) {
			Object c = args[i];
			if (c == nr) {
				if (buf != null) 
					rows.add(buf.toArray());
				buf = new ArrayList<Object>();
			} else if (buf == null) {
				throw new IllegalArgumentException("Row values must start with nr");
			} else 
				buf.add(c);
		}
		if (buf != null)
			rows.add(buf.toArray());
		return rows.toArray();
	}

	public static String buildDBName(String siteKern, int i, String dbName) {
		return siteKern + i + "_" + dbName;
	}
	
	protected String generateCreateTable(UserTable ut) throws PEException {
		StringBuffer createSql = new StringBuffer().append("CREATE ");

        createSql.append("TABLE ")
				.append(Singletons.require(HostService.class).getDBNative().getNameForQuery(ut))
				.append(" (");
		
		for ( UserColumn uc : ut.getUserColumns() ) {
			if ( uc.getOrderInTable() > 1 ) {
				createSql.append(',');
			}
            createSql.append(Singletons.require(HostService.class).getDBNative().getNameForQuery(uc));

            createSql.append(' ').append(Singletons.require(HostService.class).getDBNative().getDataTypeForQuery(uc));
			
			if ( uc.isAutoGenerated() ) {
				createSql.append(" AUTO_INCREMENT");
			}
			
			if ( !uc.isNullable() ) {
				createSql.append(" NOT NULL");
			}
			
		}
		createSql.append(") DEFAULT CHARSET=UTF8");

		return createSql.toString();
	}
	
	protected static void removeUser(ConnectionResource rootConnection, String username, String host1) throws Throwable {
		String accessSpec = "'" + username + "'@'" + host1 + "'";
		try {
			rootConnection.execute("drop user " + accessSpec);
		} catch (SQLException se) {
			// re.printStackTrace();
			// ignore: reasons: the user doesn't exist, or the user exists on some but not all sites
			assertException(se, SQLException.class,
					"SchemaException: User " + accessSpec + " does not exist");
		} catch (PEException pe) {
			// re.printStackTrace();
			// ignore: reasons: the user doesn't exist, or the user exists on some but not all sites
			assertSchemaException(pe,"User " + accessSpec + " does not exist");
		}
		DBHelper dbh = PETest.buildHelper();
		removeUser(dbh, username, host1);
		dbh.disconnect();
	}
	
	protected static void removeUser(DBHelper dbh, String username, String host1) throws Throwable {
		try {
			dbh.executeQuery("drop user '" + username + "'@'" + host1 + "'");
		} catch (SQLException se) {
			// this is expected if the user doesn't exist
		}
		dbh.executeQuery("flush privileges");
	}

	private static boolean matchingDDL(TestResource tr, boolean nativeDDL) {
		if (nativeDDL && tr.getDDL().isNative())
			return true;
		else if (!nativeDDL && !tr.getDDL().isNative())
			return true;
		return false;
	}
	
	protected static String getPlan(String lineIn, TestResource lr, TestResource rr, boolean nativePlan) {
		String exp = "EXPLAIN " + lineIn;
		TestResource expRes = null;
		if (lr != null && matchingDDL(lr,nativePlan)) {
			expRes = lr;
		} else if (rr != null && matchingDDL(rr, nativePlan)) {
			expRes = rr;
		}
		if (expRes == null)
			return null;
		try {
			ConnectionResource cr = expRes.getConnection();
			String plan = cr.printResults(exp, "plan", false);
			return plan;
		} catch (Throwable t) {
			t.printStackTrace();
			return null;
		}		
	}
	
	public static Throwable annotateFailureWithPlan(Throwable t, String lineIn, TestResource lr,
			TestResource rr, LineTag lt) {
		StringBuffer nm = new StringBuffer();
		nm.append(t.getMessage());
		nm.append(PEConstants.LINE_SEPARATOR);
		nm.append("PE Plan");
		nm.append(PEConstants.LINE_SEPARATOR);
		nm.append(getPlan(lineIn, lr, rr, false));
		nm.append(PEConstants.LINE_SEPARATOR);
		if (LineTag.UPDATE != lt) {
			nm.append("Native Plan");
			nm.append(PEConstants.LINE_SEPARATOR);
			nm.append(getPlan(lineIn, lr, rr, true));
		}
		return new Throwable(nm.toString(), t);
	}	

	
	protected static class StatementMirrorProc extends MirrorProc {

		protected String[] stmts;
		
		protected StatementMirrorProc(boolean sys, boolean check, String ...sql) {
			super(sys,check, sql[0]);
			stmts = sql;
		}
		
		public StatementMirrorProc(String ...sql) {
			super();
			stmts = sql;
		}
		
		@Override
		public String describe() {
			return stmts[0];
		}
		
		@Override
		public ResourceResponse execute(TestResource mr) throws Throwable {
			ResourceResponse er = null;
			if (mr == null) return er;
			for(String s : stmts) try {
				er = mr.getConnection().execute(s);
			} catch (Throwable t) {
				throw new Throwable("Failed to execute: '" + s + "': " + t.getMessage(), t);
			}
			return er;
		}
		
	}
	
	protected static class StatementMirrorProcIgnore extends StatementMirrorProc {
		
		protected StatementMirrorProcIgnore(String ...sql) {
			super(sql);
		}

		@Override
		public void execute(TestResource checkdb, TestResource sysdb) throws Throwable {
			if (docheck)
				execute(checkdb);
			if (dosys)
				execute(sysdb);
		}

		
	}
	
	protected static class StatementMirrorFun extends MirrorFun {
		
		protected String command;

		public StatementMirrorFun(boolean ordered, boolean ignoremd, String stmt) {
			super(ordered,ignoremd,false);
			command = stmt;
		}
		
		public StatementMirrorFun(boolean ordered, String stmt) {
			this(ordered,false,stmt);
		}
		
		public StatementMirrorFun(String stmt) {
			this(false, stmt);
		}
		
		@Override
		public String getContext() {
			return command;
		}
		
		@Override
		public ResourceResponse execute(TestResource mr) throws Throwable {
			if (mr == null) return null;
			return mr.getConnection().fetch(command);
		}
		
	}

	protected static class StatementMirrorApply extends MirrorApply {
		
		private String command;
		
		protected StatementMirrorApply(String stmt) {
			super(new LineInfo(0,null,0), stmt, null, null, false);
			command = stmt;
		}
		
		protected String getContext() {
			return command;
		}
		
	}

	protected static List<Throwable> findExceptionsOfType(Class<?> k, Throwable t) {
		ArrayList<Throwable> buf = new ArrayList<Throwable>();
		Throwable c = t;
		while(c != null) {
			if (k.isInstance(c))
				buf.add(c);
			c = c.getCause();
		}
		return buf;
	}
	
	protected static void assertException(Throwable t, Class<?> k, String message) throws Throwable {
		List<Throwable> matching = findExceptionsOfType(k,t);
		for(Throwable m : matching)
			if (message.equals(m.getMessage()))
				return;
		throw t;
	}
	
	protected static void assertSchemaException(Throwable t, String message) throws Throwable {
		assertException(t,SchemaException.class,message);
	}
	
	protected static boolean assertPEException(Throwable t, String message) throws Throwable {
		Throwable c = t;
		while(c != null) {
			if (c instanceof PEException && message.equals(c.getMessage()))
				return true;
			c = c.getCause();
		}
		return false;
	}
	
	public static Object getIgnore() {
		return ignore;
	}


	public static class TempTableChecker {
		
		private DBHelper[] conns;
		private int nsites;
		private String kern;
		private String dbName;
		private HashSet<String> lost;
		
		public TempTableChecker(String kern, int nsites, String dbName) {
			this.nsites = nsites;
			this.kern = kern;
			this.dbName = dbName;
			this.conns = null;
			lost = new HashSet<String>();
		}

		private String performCheck(boolean record) throws Throwable {
			if (conns == null)
				conns = buildConnections();
			for(int i = 0; i < conns.length; i++) {
				boolean res = conns[i].executeQuery("SHOW TABLES LIKE 'temp%'");
				if (res) {
					ResultSet rs = conns[i].getResultSet();
					while(rs.next()) {
						String name = rs.getString(1);
						if (record)
							lost.add(name);
						else if (!lost.contains(name)) {
							rs.close();
							return "Found temp table " + name + " in db " + buildDBName(kern, i + 1, dbName);
						}							
					}
					rs.close();
				}
			}
			return null;
		}
		
		public String check() throws Throwable {
			return performCheck(false);
		}

		protected void record() throws Throwable {
			performCheck(true);
		}

		protected void dumpLost() {
			for(String s : lost)
				System.out.println(s);
		}
		
		private DBHelper[] buildConnections() throws Throwable {
			DBHelper[] res = new DBHelper[nsites];
			for(int i = 0; i < nsites; i++) {
				DBHelper dbHelper = PETest.buildHelper();
				String pdbn = buildDBName(kern,i + 1,dbName);
				dbHelper.executeQuery("use " + pdbn);
				res[i] = dbHelper;
			}
			return res;
		}

		public void close() {
			for(int i = 0; i < conns.length; i++)
				conns[i].disconnect();
			conns = null;
		}
	}
	
	public static void setTemplateModeOptional() throws Exception {
		try (ProxyConnectionResource pcr = new ProxyConnectionResource()) {
			pcr.execute(buildAlterTemplateModeStmt(TemplateMode.OPTIONAL));
		} catch (final Throwable t) {
			throw new Exception("Could not set the template mode.", t);
		}
	}

	public static String buildAlterTemplateModeStmt(final TemplateMode mode) {
		return "alter dve set " + SchemaVariableConstants.TEMPLATE_MODE_NAME + " = '" + mode + "'";
	}
}
