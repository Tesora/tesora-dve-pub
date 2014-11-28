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

import com.tesora.dve.concurrent.SynchronousListener;
import io.netty.channel.EventLoopGroup;
import io.netty.util.CharsetUtil;

import java.nio.charset.Charset;
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
import com.tesora.dve.db.mysql.MysqlConnection;
import com.tesora.dve.db.mysql.MysqlPrepareStatementCollector;
import com.tesora.dve.db.mysql.MysqlMessage;
import com.tesora.dve.db.mysql.MysqlStmtCloseCommand;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtCloseRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.worker.AdditionalConnectionInfo;
import com.tesora.dve.worker.MysqlPreparedStmtExecuteCollector;
import com.tesora.dve.worker.MysqlTextResultChunkProvider;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;

public class MysqlConnectionResource extends ConnectionResource {

	private String url;
	private String userName;
	private String password;
	private boolean connected = false;
	private final boolean useUTF8;
	private Charset encoding;
	
	private MysqlConnection mysqlConn;

	public MysqlConnectionResource() throws Throwable {
		this(true);
	}

	public MysqlConnectionResource(final boolean useUTF8) throws Throwable {
		super(null);
		
		this.useUTF8 = useUTF8;

		Properties catalogProps = TestCatalogHelper.getTestCatalogProps(PETest.class);
		String portalPort = catalogProps.getProperty(PEConstants.MYSQL_PORTAL_PORT_PROPERTY, PEConstants.MYSQL_PORTAL_DEFAULT_PORT);

		PEUrl peurl = PEUrl.fromUrlString(catalogProps.getProperty(DBHelper.CONN_URL));
		peurl.setQueryOptions(new Properties());
		peurl.setPort(portalPort);
		init(peurl.getURL(), catalogProps.getProperty(DBHelper.CONN_USER), catalogProps.getProperty(DBHelper.CONN_PASSWORD), useUTF8);
	}

	private MysqlConnectionResource(String url, String userName, String password, final boolean useUTF8) throws Throwable {
		super(null);
		this.useUTF8 = useUTF8;
		init(url, userName, password, useUTF8);
	}

	private void init(String url, String userName, String password, boolean useUTF8) throws Throwable {
		this.url = url;
		this.userName = userName;
		this.password = password;

		if (useUTF8) {
			this.encoding = CharsetUtil.UTF_8;
			addPostConnectCmd("SET NAMES utf8");
		} else {
			this.encoding = CharsetUtil.ISO_8859_1;
			addPostConnectCmd("SET NAMES latin1");
		}
	
		mysqlConn = new MysqlConnection(new TestStorageSite());
	
		connect();
	}

	@Override
	public ResourceResponse execute(LineInfo info, String stmt)	throws Throwable {
		return execute(new SQLCommand(this.encoding, stmt));
	}

	public ResourceResponse execute(LineInfo info, Charset encoding, byte[] stmt) throws Throwable {
		return execute(new SQLCommand(new GenericSQLCommand(encoding, stmt)));
	}

	private ResourceResponse execute(SQLCommand sqlc) throws Throwable {
		MysqlTextResultChunkProvider results = new MysqlTextResultChunkProvider();

        PEDefaultPromise<Boolean> promise = new PEDefaultPromise<Boolean>();
        results.getDispatchBundle(mysqlConn, sqlc, promise).writeAndFlush(mysqlConn);
		SynchronousListener.sync(promise);
		
		return new ProxyConnectionResourceResponse(results);
	}
	
	@Override
	public Object prepare(LineInfo info, String stmt) throws Throwable {

		MysqlPrepareStatementCollector collector = new MysqlPrepareStatementCollector();

        PEDefaultPromise<Boolean> promise = new PEDefaultPromise<Boolean>();		
        collector.getDispatchBundle(mysqlConn, new SQLCommand(this.encoding, stmt), promise).writeAndFlush(mysqlConn);;
		SynchronousListener.sync(promise);
		return collector.getPreparedStatement();
	}

	@Override
	public ResourceResponse executePrepared(Object id, List<Object> parameters)
			throws Throwable {
		@SuppressWarnings("unchecked")
		MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt = (MyPreparedStatement<MysqlGroupedPreparedStatementId>) id; 

		MysqlPreparedStmtExecuteCollector collector = new MysqlPreparedStmtExecuteCollector(pstmt);
		
		SQLCommand sqlc = new SQLCommand(new GenericSQLCommand(this.encoding, "EXEC PREPARED"), parameters);
        PEDefaultPromise<Boolean> promise = new PEDefaultPromise<Boolean>();
        collector.getDispatchBundle(mysqlConn, sqlc, promise).writeAndFlush(mysqlConn);
		SynchronousListener.sync(promise);
		
		return new ProxyConnectionResourceResponse(collector);
	}

	@Override
	public void destroyPrepared(Object id) throws Throwable {
		@SuppressWarnings("unchecked")

		MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt = (MyPreparedStatement<MysqlGroupedPreparedStatementId>) id;
        int preparedID = (int)pstmt.getStmtId().getStmtId(mysqlConn.getPhysicalID());
        MysqlMessage message = MSPComStmtCloseRequestMessage.newMessage(preparedID);
        mysqlConn.writeAndFlush(message, new MysqlStmtCloseCommand(preparedID, new PEDefaultPromise<Boolean>()));
    }

	@Override
	public ResourceResponse fetch(LineInfo info, String stmt) throws Throwable {
		return execute(info, stmt);
	}

	@Override
	public void connect() throws Throwable {
		mysqlConn.connect(url, userName, password, ClientCapabilities.DEFAULT_PSITE_CAPABILITIES);
		connected = true;

		executePostConnectCmds();
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
		return new MysqlConnectionResource(this.url, this.userName, this.password, this.useUTF8);
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
		public Worker createWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, EventLoopGroup preferredEventLoop) throws PEException {
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
