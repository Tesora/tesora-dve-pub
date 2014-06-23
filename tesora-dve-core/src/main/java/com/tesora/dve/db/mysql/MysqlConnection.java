package com.tesora.dve.db.mysql;

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

import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.common.DBType;
import com.tesora.dve.db.mysql.portal.protocol.MyBackendDecoder;
import com.tesora.dve.db.mysql.portal.protocol.MysqlClientAuthenticationHandler;
import com.tesora.dve.db.mysql.portal.protocol.MSPEncoder;
import com.tesora.dve.db.mysql.portal.protocol.MSPProtocolEncoder;
import com.tesora.dve.exceptions.PESQLStateException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.base.Objects;
import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.concurrent.PEDefaultThreadFactory;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.DevXid;
import com.tesora.dve.worker.UserCredentials;

public class MysqlConnection implements DBConnection, DBConnection.Monitor {

	public static final boolean USE_POOLED_BUFFERS = Boolean.getBoolean("com.tesora.dve.netty.usePooledBuffers");

	private static final boolean PACKET_LOGGER = Boolean.getBoolean("MysqlConnection.packetLogger");

	// TODO what's the general strategy for mapping MySQL error codes? (see PE-5)
	private static final int MYSQL_ERR_QUERY_INTERRUPTED = 1317;

	static Logger logger = Logger.getLogger( MysqlConnection.class );

	private long shutdownQuietPeriod = 2;
	private long shutdownTimeout = 15;

	public static class Factory implements DBConnection.Factory {
		@Override
		public DBConnection newInstance(StorageSite site) {
			return new MysqlConnection(site);
		}
	}

	Bootstrap mysqlBootstrap;
	EventLoopGroup connectionEventGroup;
	private Channel channel;
	private ChannelFuture pendingConnection;
	private MysqlClientAuthenticationHandler authHandler;
	final PEPromise<Boolean> sharedExceptionCatcher;
	private Exception pendingException = null;
	private final StorageSite site;

	private boolean pendingUpdate = false;
	private boolean hasActiveTransaction = false;

	public MysqlConnection(StorageSite site) {
		sharedExceptionCatcher = getExceptionCatcher();
		this.site = site;
	}

	public void setShutdownQuietPeriod(final long seconds) {
		this.shutdownQuietPeriod = seconds;
	}

	public void setShutdownTimeout(final long seconds) {
		this.shutdownTimeout = seconds;
	}

	public long getShutdownQuietPeriod() {
		return this.shutdownQuietPeriod;
	}

	public long getShutdownTimeout() {
		return this.shutdownTimeout;
	}

	@Override
	public void connect(String url, final String userid, final String password) throws PEException {
		PEUrl peUrl = PEUrl.fromUrlString(url);

		if (!"mysql".equalsIgnoreCase(peUrl.getSubProtocol()))
			throw new PEException(MysqlConnection.class.getSimpleName() +
					" does not support the sub protocol of url \"" + url + "\"");

		InetSocketAddress serverAddress = new InetSocketAddress(peUrl.getHost(), peUrl.getPort());
        final MyBackendDecoder.CharsetDecodeHelper charsetHelper = new CharsetDecodeHelper();
		connectionEventGroup = new NioEventLoopGroup(1, new PEDefaultThreadFactory("mysql-" + site.getName()));
		mysqlBootstrap = new Bootstrap();
		mysqlBootstrap // .group(inboundChannel.eventLoop())
		.channel(NioSocketChannel.class)
		.group(connectionEventGroup)
		.option(ChannelOption.ALLOCATOR, USE_POOLED_BUFFERS ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT)
		.handler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel ch) throws Exception {
				authHandler = new MysqlClientAuthenticationHandler(new UserCredentials(userid, password), NativeCharSetCatalog.getDefaultCharSetCatalog(DBType.MYSQL));

				if (PACKET_LOGGER)
					ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));

				ch.pipeline()
				.addLast(authHandler)
                .addLast(MSPProtocolEncoder.class.getSimpleName(), new MSPProtocolEncoder())
                .addLast(MSPEncoder.class.getSimpleName(), MSPEncoder.getInstance())
                .addLast(MyBackendDecoder.class.getSimpleName(), new MyBackendDecoder(site.getName(),charsetHelper))
				.addLast(MysqlCommandSenderHandler.class.getSimpleName(), new MysqlCommandSenderHandler(site.getName()));
			}
		});

		pendingConnection = mysqlBootstrap.connect(serverAddress);

//		System.out.println("Create connection: Allocated " + totalConnections.incrementAndGet() + ", active " + activeConnections.incrementAndGet());
		channel = pendingConnection.channel();
//		channel.closeFuture().addListener(new GenericFutureListener<Future<Void>>() {
//			@Override
//			public void operationComplete(Future<Void> future) throws Exception {
//				System.out.println(channel + " is closed");
//			}
//		});
	}

	@Override
	public void execute(SQLCommand sql, DBResultConsumer consumer) throws PESQLException {
		execute(sql, consumer, sharedExceptionCatcher);
	}

	@Override
	public PEFuture<Boolean> execute(SQLCommand sql, DBResultConsumer consumer, PEPromise<Boolean> promise) throws PESQLException {
		PEFuture<Boolean> executionResult = promise;

		PEThreadContext.pushFrame(getClass().getSimpleName())
				.put("site", site.getName())
				.put("connectionId", getConnectionId())
				.put("channel", channel.toString())
				.put("promise", promise)
				.put("resultConsumer", consumer);
		if (logger.isDebugEnabled())
			    PEThreadContext.put("sql", sql.getRawSQL());
		PEThreadContext.logDebug();
		try {

			if (logger.isDebugEnabled())
				logger.debug(this.getClass().getSimpleName()
						+"@"+System.identityHashCode(this)
						+"{"+site.getName()+"}.execute("+sql.getRawSQL()+","+consumer+","+promise+")");

			if (!channel.isOpen()) {
				promise.failure(new PECommunicationsException("Channel closed: " + channel));
			} else if (pendingException == null) {
				syncToServerConnect();
				authHandler.assertAuthenticated();
				executionResult = consumer.writeCommandExecutor(channel, site, this, sql, promise);
				channel.flush();
			} else {
				promise.failure(new PEException("Queued exception from previous execution", pendingException));
				pendingException = null;
			}

			return executionResult;
		} finally {
			PEThreadContext.popFrame();
		}
	}

	private void syncToServerConnect() {
		if (pendingConnection != null) {
			pendingConnection.syncUninterruptibly();
			pendingConnection = null;
		}
	}

	@Override
	public synchronized void close() {
		if (connectionEventGroup != null) {
//			if (channel.isOpen()) {
//				ChannelFuture f = channel.write(MysqlQuitCommand.INSTANCE);
//				channel.flush();
//				f.syncUninterruptibly();
//				channel.close().syncUninterruptibly();
//			}
//			System.out.println("Close connection: Allocated " + totalConnections.get() + ", active " + activeConnections.decrementAndGet());
			connectionEventGroup.shutdownGracefully(shutdownQuietPeriod, shutdownTimeout, TimeUnit.SECONDS);
			connectionEventGroup = null;
		}
	}

	@Override
	public void start(DevXid xid) throws Exception {
		hasActiveTransaction = true;
		execute(new SQLCommand("XA START " + xid.getMysqlXid()), DBEmptyTextResultConsumer.INSTANCE);
	}

	@Override
	public void end(DevXid xid) throws Exception {
		execute(new SQLCommand("XA END " + xid.getMysqlXid()), DBEmptyTextResultConsumer.INSTANCE);
	}

	@Override
	public void prepare(DevXid xid) throws Exception {
		execute(new SQLCommand("XA PREPARE " + xid.getMysqlXid()), DBEmptyTextResultConsumer.INSTANCE,
				new PEDefaultPromise<Boolean>()).sync();
	}

	@Override
	public void commit(DevXid xid, boolean onePhase) throws Exception {
		String sql = "XA COMMIT " + xid.getMysqlXid();
		if (onePhase)
			sql += " ONE PHASE";
		execute(new SQLCommand(sql), DBEmptyTextResultConsumer.INSTANCE, new PEDefaultPromise<Boolean>()).sync();
		pendingUpdate = false;
		hasActiveTransaction = false;
	}
	@Override
	public void rollback(DevXid xid) throws Exception {
        try{
		    execute(new SQLCommand("XA ROLLBACK " + xid.getMysqlXid()), DBEmptyTextResultConsumer.INSTANCE,
				new PEDefaultPromise<Boolean>()).sync();
        } catch (PEException e){
            Exception root = e.rootCause();
            if (root instanceof PESQLStateException){
                PESQLStateException mysqlError = (PESQLStateException)root;
                //watch for rollback failing because we don't actually have an XA transaction. -sgossard
                if (backendErrorImpliesNoOpenXA(mysqlError))
                    logger.warn("tried to rollback transaction, but response implied no XA exists, " + mysqlError.getMessage());
                else
                    throw root;
            }
        }
		pendingUpdate = false;
		hasActiveTransaction = false;
	}

    public boolean backendErrorImpliesNoOpenXA(PESQLStateException mysqlError) {
        return mysqlError.getErrorNumber() == 1397 || //XAER_NOTA, XA id is completely unknown.
               (mysqlError.getErrorNumber() == 1399 && mysqlError.getErrorMsg().contains("NON-EXISTING")) //XAER_RMFAIL, because we are in NON-EXISTING state
        ;
    }

    @Override
	public void setCatalog(String databaseName) throws Exception {
		execute(new SQLCommand("use " + databaseName), DBEmptyTextResultConsumer.INSTANCE,
				sharedExceptionCatcher);
	}

	@Override
	public void cancel() {
		try {
			execute(new SQLCommand("KILL QUERY " + authHandler.getThreadID()), MysqlKillResultDiscarder.INSTANCE);
		} catch (PESQLException e) {
			logger.warn("Failed to cancel active query", e);
		}
	}

	private PEPromise<Boolean> getExceptionCatcher() {

		// re-usable promise that forwards success/failure calls; result is ignored
		PEDefaultPromise<Boolean> catcher = new PEDefaultPromise<Boolean>() {
			@Override
			public PEDefaultPromise<Boolean> success(Boolean returnValue) {
				onSuccess(returnValue);
				return this;
			}

			@Override
			public PEDefaultPromise<Boolean> failure(Exception e) {
				onFailure(e);
				return this;
			}

		};
		return catcher.addListener(this);
	}

	@Override
	public void onFailure(Exception e) {
		pendingException = new PEException("Unhandled exception received on server housekeeping sql statement", e);
	}

	@Override
	public void onSuccess(Boolean returnValue) {
	}

	@Override
	public boolean hasPendingUpdate() {
		return pendingUpdate;
	}

	@Override
	public void onUpdate() {
		pendingUpdate  = true;
	}

	@Override
	public boolean hasActiveTransaction() {
		return hasActiveTransaction;
	}

	@Override
	public int getConnectionId() {
		return (authHandler != null) ? authHandler.getThreadID() : 0;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("site", this.site.getName())
				.add("id", this.getConnectionId())
				.toString();
	}

}
