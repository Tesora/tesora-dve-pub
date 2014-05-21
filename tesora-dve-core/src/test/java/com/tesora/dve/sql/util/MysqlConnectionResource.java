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

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.MysqlStmtCloseDiscarder;
import com.tesora.dve.db.mysql.MysqlConnection;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import com.tesora.dve.db.mysql.MysqlPrepareStatementCollector;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.worker.MysqlPreparedStmtExecuteCollector;
import com.tesora.dve.worker.MysqlTextResultChunkProvider;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

public class MysqlConnectionResource extends ConnectionResource {

	private String url;
	private String userName;
	private String password;
	private boolean connected = false;
	
	private MysqlConnection mysqlConn;

	public MysqlConnectionResource() throws Throwable {
		super(null);
		
		Properties catalogProps = TestCatalogHelper.getTestCatalogProps(PETest.class);
		String portalPort = catalogProps.getProperty(PEConstants.MYSQL_PORTAL_PORT_PROPERTY, PEConstants.MYSQL_PORTAL_DEFAULT_PORT);

		PEUrl peurl = PEUrl.fromUrlString(catalogProps.getProperty(DBHelper.CONN_URL));
		peurl.setPort(portalPort);
		init(peurl.getURL(),catalogProps.getProperty(DBHelper.CONN_USER),catalogProps.getProperty(DBHelper.CONN_PASSWORD));
	}

	public MysqlConnectionResource(String url, String userName, String password) throws Throwable {
		super(null);
		init(url, userName, password);
	}

	private void init(String url, String userName, String password) throws Throwable {
		this.url = url;
		this.userName = userName;
		this.password = password;
	
		mysqlConn = new MysqlConnection(new TestStorageSite());
	
		connect();
	}

	@Override
	public ResourceResponse execute(LineInfo info, String stmt)	throws Throwable {
		return execute(new SQLCommand(stmt));
	}

	public ResourceResponse execute(LineInfo info, byte[] stmt)	throws Throwable {
		return execute(new SQLCommand(new GenericSQLCommand(stmt)));
	}

	private ResourceResponse execute(SQLCommand sqlc) throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();

		mysqlConn.execute(sqlc, results, new PEDefaultPromise<Boolean>()).sync();
		
		return new ProxyConnectionResourceResponse(results);
	}
	
	@Override
	public Object prepare(LineInfo info, String stmt) throws Throwable {

		MysqlPrepareStatementCollector collector = new MysqlPrepareStatementCollector();
		
		mysqlConn.execute(new SQLCommand(stmt), collector, new PEDefaultPromise<Boolean>()).sync();
		return collector.getPreparedStatement();
	}

	@Override
	public ResourceResponse executePrepared(Object id, List<Object> parameters)
			throws Throwable {
		@SuppressWarnings("unchecked")
		MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt = (MyPreparedStatement<MysqlGroupedPreparedStatementId>) id; 

		MysqlPreparedStmtExecuteCollector collector = new MysqlPreparedStmtExecuteCollector(pstmt);
		
		SQLCommand sqlc = new SQLCommand(new GenericSQLCommand("EXEC PREPARED"), parameters);
		mysqlConn.execute(sqlc, collector, new PEDefaultPromise<Boolean>()).sync();
		
		return new ProxyConnectionResourceResponse(collector);
	}

	@Override
	public void destroyPrepared(Object id) throws Throwable {
		@SuppressWarnings("unchecked")
		MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt = (MyPreparedStatement<MysqlGroupedPreparedStatementId>) id; 
		MysqlStmtCloseDiscarder discarder = new MysqlStmtCloseDiscarder(pstmt);
		
		mysqlConn.execute(new SQLCommand(new GenericSQLCommand("CLOSE PREP STMT")), discarder);
	}

	@Override
	public ResourceResponse fetch(LineInfo info, String stmt) throws Throwable {
		return execute(info, stmt);
	}

	@Override
	public void connect() throws Throwable {
		mysqlConn.connect(url, userName, password);
		connected = true;
	}

	@Override
	public void disconnect() throws Throwable {
		mysqlConn.close();
		mysqlConn = null;
		connected = false;
	}

	public MysqlConnection getConnection() {
		return mysqlConn;
	}

	@Override
	public boolean isConnected() throws Throwable {
		return connected;
	}

	@Override
	public String describe() {
		return mysqlConn.toString();
	}

	@Override
	public ConnectionResource getNewConnection() throws Throwable {
		return new MysqlConnectionResource(this.url, this.userName, this.password);
	}

	@Override
	public ExceptionClassification classifyException(Throwable t) {
		return null;
	}

	class TestStorageSite implements StorageSite {

		@Override
		public String getName() {
			return null;
		}

		@Override
		public String getMasterUrl() {
			return null;
		}

		@Override
		public String getInstanceIdentifier() {
			return null;
		}

		@Override
		public PersistentSite getRecoverableSite(CatalogDAO c) {
			return null;
		}

		@Override
		public Worker pickWorker(Map<StorageSite, Worker> workerMap)
				throws PEException {
			return null;
		}

		@Override
		public void annotateStatistics(LogSiteStatisticRequest sNotice) {
		}

		@Override
		public void incrementUsageCount() {
		}

		@Override
		public Worker createWorker(UserAuthentication auth) throws PEException {
			return null;
		}

		@Override
		public void onSiteFailure(CatalogDAO c) throws PEException {
		}

		@Override
		public int getMasterInstanceId() {
			return 0;
		}

		@Override
		public boolean supportsTransactions() {
			return false;
		}

		@Override
		public boolean hasDatabase(UserVisibleDatabase ctxDB) {
			return false;
		}

		@Override
		public void setHasDatabase(UserVisibleDatabase ctxDB) {
			// TODO Auto-generated method stub
			
		}
		
		
	}
}
