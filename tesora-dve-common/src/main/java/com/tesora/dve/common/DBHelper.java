// OS_STATUS: public
package com.tesora.dve.common;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public class DBHelper {
	public static final String CONN_ROOT = PEConstants.PROP_JPA_JDBC_PREFIX;

	public static final String CONN_DRIVER_CLASS = PEConstants.PROP_FULL_JDBC_DRIVER;
	public static final String CONN_URL = PEConstants.PROP_FULL_JDBC_URL;
	public static final String CONN_USER = PEConstants.PROP_FULL_JDBC_USER;
	public static final String CONN_PASSWORD = PEConstants.PROP_FULL_JDBC_PASSWORD;
	public static final String CONN_DBNAME = PEConstants.PROP_DBNAME;

	private static Logger logger = Logger.getLogger(DBHelper.class);

	protected PEUrl connUrl;
	protected String connUserName;
	protected String connPwd;
	protected String connDBName = null;

	protected Connection connection = null;
	protected Statement stmt = null;
	protected PreparedStatement prepStmt = null;
	protected String prepQuery;

	protected int rowCount = 0;
	protected long lastInsertID = 0L;
	protected ResultSet resultSet = null;

	protected boolean enableResultSetStreaming = false;
	protected boolean useBufferedQuery = false;
	protected boolean disconnectAfterProcess = true;

	/**
	 * @param props
	 * @throws PEException
	 */
	public DBHelper(Properties props) throws PEException {
		this(props.getProperty(CONN_URL), props.getProperty(CONN_USER), props.getProperty(CONN_PASSWORD), props
				.getProperty(CONN_DBNAME));
	}

	/**
	 * @param url
	 * @param userName
	 * @param password
	 * @param database
	 * @throws PEException
	 */
	public DBHelper(String url, String userName, String password, String database) throws PEException {
		this(PEUrl.fromUrlString(url), userName, password, database);
	}

	/**
	 * @param url
	 * @param userName
	 * @param password
	 * @throws PEException
	 */
	public DBHelper(String url, String userName, String password) throws PEException {
		this(PEUrl.fromUrlString(url), userName, password);
	}

	/**
	 * @param url
	 * @param userName
	 * @param password
	 * @param database
	 * @throws PEException
	 */
	public DBHelper(PEUrl url, String userName, String password, String database) throws PEException {
		this(url, userName, password);

		this.connDBName = database;
	}

	/**
	 * @param url
	 * @param userName
	 * @param password
	 * @throws PEException
	 */
	public DBHelper(PEUrl url, String userName, String password) throws PEException {
		this.connUrl = url;
		this.connUserName = userName;
		this.connPwd = password;
		this.connDBName = connUrl.getPath();
	}

	public static DBType urlToDBType(String url) throws PEException {
		return DBType.fromDriverClass(urlToDriverClass(PEUrl.fromUrlString(url)));
	}

	public static String urlToDriverClass(String url) throws PEException {
		return urlToDriverClass(PEUrl.fromUrlString(url));
	}

	public static String urlToDriverClass(PEUrl url) throws PEException {
		if (url.getSubProtocol().equalsIgnoreCase(PEConstants.MYSQL_SUBPROTOCOL))
			return PEConstants.MYSQL_DRIVER_CLASS;

		throw new PEException("Cannot determine JDBC driver class for '" + url.getSubProtocol() + "'");
	}

	public static String loadDriverForURL(String url) throws PEException {
		return loadDriver(urlToDriverClass(url));
	}

	public static String loadDriverForURL(PEUrl url) throws PEException {
		return loadDriver(urlToDriverClass(url));
	}

	public static String loadDriver(String driverClass) throws PEException {
		try {
			Class.forName(driverClass);
		} catch (Exception e) {
			logger.error("JDBC driver class '" + driverClass + "' not found");

			throw new PEException("JDBC driver class '" + driverClass + "' not found");
		}
		return driverClass;
	}

	public Connection getConnection() {
		return this.connection;
	}

	public PEUrl getUrl() {
		return connUrl;
	}

	public String getUserName() {
		return connUserName;
	}

	public String getPassword() {
		return connPwd;
	}

	public String getDBName() {
		return connDBName;
	}

	public void setDBName(String connDBName) {
		this.connDBName = connDBName;
	}

	public void disconnect() {
		logger.info("Disconnecting from " + this.connUrl + " as " + this.connUserName);

		try {
			closeStatement();

			if (connection != null)
				connection.close();
		} catch (SQLException e) {
			logger.error(e, e);
		}
	}

	public DBHelper connect() throws PEException {
		return connect(new Properties());
	}

	public DBHelper connect(Properties props) throws PEException {
		props.put("user", connUserName);
		props.put("password", connPwd);

		return doConnect(props);
	}

	private DBHelper doConnect(Properties props) throws PEException {

		DBHelper.loadDriverForURL(connUrl);

		try {
			logger.info("Connecting to " + this.connUrl + " as " + this.connUserName);

			connection = DriverManager.getConnection(connUrl.getURL(), props);
		} catch (Exception se) {
			throw new PEException("Error connecting to database '" + connUrl + "' - " + se.getMessage(), se);
		}

		try {
			if (this.connDBName != null) {
				connection.setCatalog(connDBName);
				logger.info("Database set to " + connDBName);
			}
		} catch (Exception se) {
			throw new PEException(
					"Error using database '" + connDBName + "' on '" + connUrl + "' - " + se.getMessage(), se);
		}
		return this;
	}

	public void closeStatement() throws SQLException {
		if (resultSet != null) {
			resultSet.close();
			resultSet = null;
		}
		if (stmt != null) {
			stmt.close();
			stmt = null;
		}
		if (prepStmt != null) {
			prepStmt.close();
			prepStmt = null;
		}
	}

	/**
	 * @param query
	 *            The query to run
	 * @return <b>true</b> if the first result is a ResultSet object;
	 *         <b>false</b> if it is an update count or there are no results
	 * @throws SQLException
	 */
	public boolean executeQuery(String query) throws SQLException {
		closeStatement();

		checkConnected();

		if (logger.isDebugEnabled())
			logger.debug("ExecuteQuery '" + query + "' on " + getUrl());

		stmt = connection.createStatement();
		lastInsertID = 0L;

		if (isEnableResultSetStreaming()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
		}
		boolean ret = stmt.execute(query, Statement.RETURN_GENERATED_KEYS);

		if (!ret) {
			// when stmt.execute returns false it means no result set is
			// expected get the number of rows affected
			rowCount = stmt.getUpdateCount();

			printLine(rowCount + " rows affected");

			ResultSet rs = stmt.getGeneratedKeys();
			if (rs.next()) {
				lastInsertID = rs.getLong(1);
			}
		} else {
			// a stmt returning a result set was run
			resultSet = stmt.getResultSet();
			if (useBufferedQuery)
				resultSet.setFetchSize(Integer.MAX_VALUE);
		}
		return ret;
	}

	/**
	 * @param line
	 */
	protected void printLine(String line) {
		// TODO - implement this?
	}

	public void executeFromStream(InputStream is) throws SQLException {
		final String cmdDelimeter = ";";
		final String comment = "--";
		String command = "";
		String separator = "";

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(is));

			String line = null;
			while ((line = reader.readLine()) != null) {

				if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
					break;
				}

				String trimmed = line.trim();
				if (trimmed.startsWith(comment))
					continue;
				if (trimmed.endsWith(cmdDelimeter)) {
					command = command + separator + line;
					try {
						executeQuery(command);
					} catch (SQLException se) {
						throw se;
					}
					command = "";
				} else {
					command = command + separator + line;
					separator = " ";
				}
			}
		} catch (IOException e) {
			logger.error(e, e);
			e.printStackTrace();
		} finally {
			if (disconnectAfterProcess)
				this.disconnect();

			if (reader != null)
				try {
					reader.close();
				} catch (Exception e) {
					// do nothing
				}
		}
	}

	public int getRowCount() {
		return rowCount;
	}

	public long getLastInsertID() {
		return lastInsertID;
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public void prepare(String query) throws SQLException {
		closeStatement();

		checkConnected();

		prepQuery = query;
		prepStmt = connection.prepareStatement(query);
	}

	public PreparedStatement prepareIndependent(String query) throws SQLException {
		if (resultSet != null) {
			resultSet.close();
			resultSet = null;
		}

		checkConnected();

		return connection.prepareStatement(query);
	}

	private boolean executePrepared(PreparedStatement ps, String query, List<Object> params) throws SQLException,
			PEException {

		if (ps == null) {
			throw new PEException("A prepared statement is not available to be executed - call prepare first");
		}

		if (logger.isDebugEnabled() && query != null)
			logger.debug("Command: " + query);

		for (int i = 0; i < params.size(); i++) {
			ps.setObject(i + 1, params.get(i));
		}

		boolean ret = ps.execute();
		if (!ret) {
			// when stmt.execute returns false it means no result set is
			// expected get the number of rows affected
			rowCount = ps.getUpdateCount();

			printLine(rowCount + " rows affected");

		} else {
			// a prepStmt returning a result set was run
			resultSet = ps.getResultSet();
			if (useBufferedQuery)
				resultSet.setFetchSize(Integer.MAX_VALUE);
		}
		return ret;

	}

	public boolean executePrepared(List<Object> params) throws SQLException, PEException {
		return executePrepared(prepStmt, prepQuery, params);
	}

	public boolean executePrepared(PreparedStatement ps, List<Object> params) throws SQLException, PEException {
		return executePrepared(ps, null, params);
	}

	public boolean isEnableResultSetStreaming() {
		return enableResultSetStreaming;
	}

	public void setEnableResultSetStreaming(boolean enableResultSetStreaming) {
		this.enableResultSetStreaming = enableResultSetStreaming;
	}

	public boolean useBufferedQuery() {
		return useBufferedQuery;
	}

	public void setBufferedQuery(boolean useBufferedQuery) {
		this.useBufferedQuery = useBufferedQuery;
	}

	public boolean disconnectAfterProcess() {
		return disconnectAfterProcess;
	}

	public void setDisconnectAfterProcess(boolean disconnectAfterProcess) {
		this.disconnectAfterProcess = disconnectAfterProcess;
	}

	public int getNumWarnings() throws SQLException {
		int count = 0;
		if (stmt != null) {
			SQLWarning warning = stmt.getWarnings();
			while (warning != null) {
				// TODO: filter for our test code until SHOW is completely fixed
				if (!StringUtils.equalsIgnoreCase(warning.getMessage(), "SHOW WARNINGS is not supported")) {
					count++;
				}
				warning = warning.getNextWarning();
			}
			return count;
		}
		return 0;
	}

	private void checkConnected() throws SQLException {
		if (connection == null)
			throw new SQLException("Not connected");
	}
}
