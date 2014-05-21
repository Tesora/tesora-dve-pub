// OS_STATUS: public
package com.tesora.dve.sql.util;

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

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.standalone.PETest;

public class DBHelperConnectionResource extends JdbcConnectionResource {

	protected DBHelper conn;
	protected Statement stmt;
	protected boolean connected;
	protected boolean useUTF8 = true;

	protected Set<PreparedStatement> prepared = new HashSet<PreparedStatement>();
	
	public DBHelperConnectionResource() throws Throwable {
		super();
		uberInit(getConnectParams());
	}

	public DBHelperConnectionResource(boolean useUTF8) throws Throwable {
		super();
		this.useUTF8 = useUTF8;
		uberInit(getConnectParams());
	}

	protected DBHelperConnectionResource(String userName, String password) throws Throwable {
		super();
		JdbcConnectParams jdbcConnParams = getConnectParams();
		jdbcConnParams.setUserid(userName);
		jdbcConnParams.setPassword(password);
		uberInit(jdbcConnParams);
	}
	
	public DBHelperConnectionResource(Properties urlConnectOptions) throws Throwable {
		super();
		JdbcConnectParams jdbcConnParams = getConnectParams();
		jdbcConnParams.setURLOptions(urlConnectOptions);
		uberInit(jdbcConnParams);
	}
	
	public DBHelperConnectionResource(String url, String userid, String password) throws Throwable {
		super();
		PEUrl peurl = PEUrl.fromUrlString(url);
		JdbcConnectParams jcp = new JdbcConnectParams(peurl.getURL(), userid, password);
		uberInit(jcp);
	}

	protected DBHelperConnectionResource(DBHelperConnectionResource other) throws Throwable {
		super(other);
		uberInit(getConnectParams());
	}
	
	private void uberInit(JdbcConnectParams jcp) throws Throwable {
		addJDBCOptions(jcp);
		initialize(jcp);
		init();
	}
	
	public void init() throws Throwable {
		if ( useUTF8 )
			this.addPostConnectCmd("SET NAMES UTF8");
		
		connected = false;
		conn = new DBHelper(getPEUrl(), getUserid(), getPassword());
		connect();
	}
	
	public void addJDBCOptions(JdbcConnectParams jdbcConnParams) throws Throwable {
		Properties props = new Properties();
		if ( useUTF8 ) {
			props.setProperty("useUnicode","yes");
			props.setProperty("characterEncoding","UTF8");
		} else {
			props.setProperty("characterEncoding","latin1");
		}
		props.setProperty("zeroDateTimeBehavior", "round");
		jdbcConnParams.setURLOptions(props);
	}
	
	public JdbcConnectParams getConnectParams() throws Throwable {
		if (connParams != null) return connParams;
		Properties catalogProps = TestCatalogHelper.getTestCatalogProps(PETest.class);
		PEUrl peurl = PEUrl.fromUrlString(catalogProps.getProperty(DBHelper.CONN_URL));

		JdbcConnectParams jcp = new JdbcConnectParams(peurl.getURL(),
				catalogProps.getProperty(DBHelper.CONN_USER),
				catalogProps.getProperty(DBHelper.CONN_PASSWORD));

		return jcp;
	}
	
	public DBHelper getHelper() {
		return conn;
	}
	
	/**
	 * @param info
	 * @param stmt1
	 * @return
	 * @throws Throwable
	 */
	private JdbcConnectionResourceResponse executeQuery(LineInfo info, String stmt1) throws Throwable {
		if (conn.executeQuery(stmt1)) {
			return new JdbcConnectionResourceResponse(conn.getResultSet(),null, 0);
		}
		return new JdbcConnectionResourceResponse(null,new Long(conn.getRowCount()), conn.getLastInsertID());
	}
	
	@Override
	public ResourceResponse execute(LineInfo info, String stmt1) throws Throwable {
		return executeQuery(info, stmt1);
	}

	@Override
	public ResourceResponse fetch(LineInfo info, String stmt1)
			throws Throwable {
		return executeQuery(info, stmt1);
	}

	@Override
	public void connect() throws Throwable {
		conn.connect();
		stmt = conn.getConnection().createStatement();
		connected = true;
		
		executePostConnectCmds();
	}

	@Override
	public void disconnect() throws Throwable {
		for(PreparedStatement ps : prepared) 
			ps.close();
		prepared.clear();
		stmt.close();
		conn.disconnect();
		connected = false;
	}

	@Override
	public boolean isConnected() {
		return connected;
	}

	@Override
	public ExceptionClassification classifyException(Throwable t) {
		if (t.getMessage() == null)
			return null;
		String m = t.getMessage().trim();
		if (StringUtils.containsIgnoreCase(m, "Table") && StringUtils.endsWithIgnoreCase(m, "doesn't exist"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(m, "Unknown table") || StringUtils.containsIgnoreCase(m, "Unknown column"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(m, "Every derived table must have its own alias"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(m, "Field") && StringUtils.endsWithIgnoreCase(m, "doesn't have a default value"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(m, "SAVEPOINT") && StringUtils.endsWithIgnoreCase(m, "does not exist"))
			return ExceptionClassification.DNE;
		if (StringUtils.containsIgnoreCase(m, "Data Truncation:"))
			return ExceptionClassification.OUT_OF_RANGE;
		if (StringUtils.containsIgnoreCase(m, "You have an error in your SQL syntax"))
			return ExceptionClassification.SYNTAX;
		if (StringUtils.containsIgnoreCase(m, "Duplicate entry"))
			return ExceptionClassification.DUPLICATE;
		if (StringUtils.containsIgnoreCase(m, "option not supported"))
			return ExceptionClassification.UNSUPPORTED_OPERATION;
		if (StringUtils.startsWithIgnoreCase(m, "File") && StringUtils.containsIgnoreCase(m, "not found"))
			return ExceptionClassification.FILE_NOT_FOUND;
		return null;
	}
	
	@Override
	public ConnectionResource getNewConnection() throws Throwable {
		return new DBHelperConnectionResource(this);
	}

	@Override
	public Statement getStatement() throws Throwable {
		return stmt;
	}

	@Override
	public String describe() {
		return (conn == null ? "null dbhelper" : "dbhelper");
	}

	@Override
	public Object prepare(LineInfo info, String stmt1) throws Throwable {
		PreparedStatement ps = conn.prepareIndependent(stmt1);
		prepared.add(ps);
		return ps;
	}

	@Override
	public ResourceResponse executePrepared(Object ps, List<Object> parameters) throws Throwable {
		if ( conn.executePrepared((PreparedStatement)ps, parameters)) {
			return new JdbcConnectionResourceResponse(conn.getResultSet(),null, 0);
		}
		return new JdbcConnectionResourceResponse(null,new Long(conn.getRowCount()), conn.getLastInsertID());
	}

	@Override
	public void destroyPrepared(Object id) throws Throwable {
		PreparedStatement ps = (PreparedStatement) id;
		ps.close();
		prepared.remove(ps);
	}

	@Override
	public boolean hasWarnings() {
		boolean hasWarnings = false;
		try {
			if (conn != null) {
				return (conn.getNumWarnings() > 0);
			}
		} catch (Exception e) {
			hasWarnings = false;
		}
		return hasWarnings;
	}

}
