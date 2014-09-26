package com.tesora.dve.dbc;

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


import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;

import io.netty.channel.ChannelHandlerContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSet;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBMetadata;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.mysql.MysqlLoadDataInfileRequestCollector;
import com.tesora.dve.db.mysql.PEMysqlErrorException;
import com.tesora.dve.errmap.ErrorMapper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.messages.AddConnectParametersExecutor;
import com.tesora.dve.server.connectionmanager.messages.ExecuteRequestExecutor;
import com.tesora.dve.server.connectionmanager.loaddata.LoadDataBlockExecutor;
import com.tesora.dve.server.connectionmanager.loaddata.LoadDataRequestExecutor;
import com.tesora.dve.worker.MysqlTextResultChunkProvider;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.UserCredentials;

public class ServerDBConnection {

	SSConnection ssCon = null;
	Long rowsAffected;

	public ServerDBConnection(String connectUser, String connectPwd) throws PEException {
		this(connectUser, connectPwd, null);
	}
	
	public ServerDBConnection(String connectUser, String connectPwd, ServerDBConnectionParameters svrDBParams) throws PEException {
		try {
			if (ssCon != null) {
				ssCon.close();
			}
			ssCon = new SSConnection();

			UserCredentials userCred = new UserCredentials(connectUser, connectPwd, true);
			ssCon.startConnection(userCred);

			if ( svrDBParams != null ) {
				ResponseMessage rm = new AddConnectParametersExecutor().execute(ssCon, new AddConnectParametersRequest(svrDBParams));
				if (!rm.isOK())
					throw new PEException(rm.getException());
			}

		} catch (Throwable t) {
			throw new PEException(t);
		}
	}

	public void closeComms() throws PEException {
		if (ssCon != null) {
			ssCon.close();
		}
	}

	public boolean isInTransaction() throws SQLException {
		return (ssCon == null) ? false : ssCon.hasActiveTransaction();
	}
	
	public SSConnection getSSConn() {
		return ssCon;
	}

	public void setCatalog(String catalog) throws SQLException {
		if (catalog != null) {
			execute("USE " + catalog);
		}
	}
	
	public boolean execute(String sql) throws SQLException {
		boolean rv = false;
		initialize();
		
		final MysqlTextResultCollector resultConsumer = new MysqlTextResultCollector();
		try {
			executeInContext(resultConsumer, sql.getBytes());
			rv = resultConsumer.hasResults();
		} catch (PEMysqlErrorException e) {
			throw new SQLException(e);
		} catch (PEException e) {
			throw new SQLException(e);
		} catch (Throwable t) {
			throw new SQLException(t);
		} finally {
			ssCon.releaseNonTxnLocks();
		}
		
		return rv;
	}
	
	public ResultSet executeQuery(String sql) throws SQLException {
		initialize();
		
		final MysqlTextResultChunkProvider resultConsumer = new MysqlTextResultChunkProvider();
		try {
			executeInContext(resultConsumer, sql.getBytes());
			return new DBCResultSet(resultConsumer, this);
		} catch (Throwable t) {
			throw new SQLException(t);
		} finally {
			ssCon.releaseNonTxnLocks();
		}
	}

	public int executeUpdate(String sql) throws SQLException {
		return executeUpdate(sql.getBytes());
	}

	public int executeUpdate(byte[] sql) throws SQLException {
		initialize();
	
		final MysqlTextResultCollector resultConsumer = new MysqlTextResultCollector();
		try {
			executeInContext(resultConsumer, sql);
			
			// Not the most optimal but it's the quickest way to use rows affected vs rows changed settings
			int ret = (int) resultConsumer.getNumRowsAffected();
			if (ret == 0 &&	resultConsumer.getInfoString() != null && !StringUtils.startsWithIgnoreCase(resultConsumer.getInfoString(), "Rows matched: 0")) {
				int start = StringUtils.indexOf(resultConsumer.getInfoString(), "Rows matched: ") + "Rows matched: ".length();
				int end = StringUtils.indexOf(resultConsumer.getInfoString(), "Changed:");
				try {
					ret = Integer.valueOf(StringUtils.substring(resultConsumer.getInfoString(), start, end).trim());
				} catch (Exception e) {
					// do nothing take original value
				}
			}
			
			return ret;
		} catch (SchemaException se) {
			throw ErrorMapper.makeException(se);
		} catch (Throwable t) {
			throw new SQLException(t);
		} finally {
			ssCon.releaseNonTxnLocks();
		}
	}

	private void executeInContext(final MysqlTextResultCollector resultConsumer, final byte[] sql)
			throws PEException, Throwable {
		Throwable t = ssCon.executeInContext(new Callable<Throwable>() {
			public Throwable call() {
				try {
					ExecuteRequestExecutor.execute(ssCon, resultConsumer, sql);
				} catch (Throwable e) {
					return e;
				}
				return null;
			}
		});
		if (t instanceof SchemaException) 
			throw (SchemaException)t;
		if (t != null && t.getCause() != null) {
			throw new PEException(t);
		}
	}
	
	public void executeLoadDataRequest(
			ChannelHandlerContext channelHandlerContext,
			byte[] query) throws SQLException {
		MysqlLoadDataInfileRequestCollector resultConsumer = new MysqlLoadDataInfileRequestCollector(channelHandlerContext);
		try {
			loadDataRequestExecuteInContext(channelHandlerContext, resultConsumer, query);
			if (resultConsumer.getFileName() == null) {
				throw new SQLException(new PEException("Cannot handle load data statement: " + new String(query)));
			}
		} catch (Throwable t) {
			throw new SQLException(t);
		}
	}
	
	private void loadDataRequestExecuteInContext(final ChannelHandlerContext channelHandlerContext, final MysqlLoadDataInfileRequestCollector resultConsumer, final byte[] query)
			throws PEException, Throwable {
		final NativeCharSet clientCharSet = MysqlNativeCharSet.UTF8;
		Throwable t = ssCon.executeInContext(new Callable<Throwable>() {
			public Throwable call() {
				try {
					LoadDataRequestExecutor.execute(channelHandlerContext, ssCon, resultConsumer, clientCharSet.getJavaCharset(), query);
				} catch (Throwable e) {
					return e;
				}
				return null;
			}
		});
		if (t != null && t.getCause() != null) {
			throw new PEException(t);
		}
	}

	public void executeLoadDataBlock(
			ChannelHandlerContext channelHandlerContext,
			byte[] readData) throws SQLException {
		try {
			loadDataBlockExecuteInContext(channelHandlerContext, readData);
		} catch (Throwable t) {
			throw new SQLException(t);
		}
	}

	private void loadDataBlockExecuteInContext(final ChannelHandlerContext channelHandlerContext, final byte[] readData)
			throws PEException, Throwable {
		Throwable t = ssCon.executeInContext(new Callable<Throwable>() {
			public Throwable call() {
				try {
					LoadDataBlockExecutor.executeInsert(channelHandlerContext, ssCon, 
							LoadDataBlockExecutor.processDataBlock(channelHandlerContext, ssCon, readData));
				} catch (Throwable e) {
					return e;
				}
				return null;
			}
		});
		if (t != null && t.getCause() != null) {
			throw new PEException(t);
		}
	}

	public NativeType findType(String nativeTypeName) throws SQLException {
		try {
			return populateDBMetadata().findType(nativeTypeName);
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	private void initialize() {
		rowsAffected = 0L;
	}
	
	private DBMetadata populateDBMetadata() {
        return Singletons.require(HostService.class).getDBNative().getDBMetadata();
	}
}
