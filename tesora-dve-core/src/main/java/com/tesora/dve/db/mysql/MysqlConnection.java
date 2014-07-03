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
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.DelegatingCompletionHandle;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.mysql.portal.protocol.*;
import com.tesora.dve.exceptions.PESQLStateException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import com.google.common.base.Objects;
import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEDefaultPromise;
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

    DeferredErrorHandle deferredErrorHandle = new DeferredErrorHandle();

	private long shutdownQuietPeriod = 2;
	private long shutdownTimeout = 15;

	public static class Factory implements DBConnection.Factory {
		@Override
		public DBConnection newInstance(EventLoopGroup eventLoop, StorageSite site) {
			return new MysqlConnection(eventLoop, site);
		}
	}

	Bootstrap mysqlBootstrap;
	EventLoopGroup connectionEventGroup;
	private Channel channel;
	private ChannelFuture pendingConnection;
	private MysqlClientAuthenticationHandler authHandler;

	private Exception pendingException = null;
	private final StorageSite site;

	private boolean pendingUpdate = false;
	private boolean hasActiveTransaction = false;

	public MysqlConnection(EventLoopGroup eventLoop, StorageSite site) {
        this.connectionEventGroup = eventLoop;
		this.site = site;
	}

    public MysqlConnection(StorageSite site) {
        this(SharedEventLoopHolder.getLoop(),site);
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
	public void connect(String url, final String userid, final String password, final long clientCapabilities) throws PEException {
		PEUrl peUrl = PEUrl.fromUrlString(url);

		if (!"mysql".equalsIgnoreCase(peUrl.getSubProtocol()))
			throw new PEException(MysqlConnection.class.getSimpleName() +
					" does not support the sub protocol of url \"" + url + "\"");

		InetSocketAddress serverAddress = new InetSocketAddress(peUrl.getHost(), peUrl.getPort());
        final MyBackendDecoder.CharsetDecodeHelper charsetHelper = new CharsetDecodeHelper();
		mysqlBootstrap = new Bootstrap();
		mysqlBootstrap // .group(inboundChannel.eventLoop())
		.channel(NioSocketChannel.class)
		.group(connectionEventGroup)
		.option(ChannelOption.ALLOCATOR, USE_POOLED_BUFFERS ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT)
		.handler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel ch) throws Exception {
				authHandler = new MysqlClientAuthenticationHandler(new UserCredentials(userid, password), clientCapabilities, NativeCharSetCatalog.getDefaultCharSetCatalog(DBType.MYSQL));           

                if (PACKET_LOGGER)
                    ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));

                ch.pipeline()
                        .addLast(authHandler)
                        .addLast(MSPProtocolEncoder.class.getSimpleName(), new MSPProtocolEncoder())
                        .addLast(MSPEncoder.class.getSimpleName(), MSPEncoder.getInstance())
                        .addLast(MyBackendDecoder.class.getSimpleName(), new MyBackendDecoder(site.getName(), charsetHelper))
                        .addLast(StreamValve.class.getSimpleName(), new StreamValve())
                        .addLast(MysqlCommandSenderHandler.class.getSimpleName(), new MysqlCommandSenderHandler(site.getName()));
            }
        });

		pendingConnection = mysqlBootstrap.connect(serverAddress);

//		System.out.println("Create connection: Allocated " + totalConnections.incrementAndGet() + ", active " + activeConnections.incrementAndGet());
		channel = pendingConnection.channel();

        //TODO: this was moved from execute to connect, which avoids blocking on the execute to be netty friendly, but causes lag on checkout.  Should make this event driven like everything else. -sgossard
        syncToServerConnect();
        authHandler.assertAuthenticated();
//		channel.closeFuture().addListener(new GenericFutureListener<Future<Void>>() {
//			@Override
//			public void operationComplete(Future<Void> future) throws Exception {
//				System.out.println(channel + " is closed");
//			}
//		});
	}

    @Override
	public void execute(SQLCommand sql, DBResultConsumer consumer, CompletionHandle<Boolean> promise) {
        CompletionHandle<Boolean> resultTracker = wrapHandler(promise);

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
//						+"@"+System.identityHashCode(this)
						+"{"+site.getName()+"}.execute("+sql.getRawSQL()+","+consumer+","+promise+")");

			if (!channel.isOpen()) {
                resultTracker.failure(new PECommunicationsException("Channel closed: " + channel));
			} else if (pendingException == null) {
				consumer.writeCommandExecutor(channel, site, this, sql, resultTracker);
                channel.flush();
			} else {
                Exception currentError = pendingException;
                pendingException = null; //if no tracker was provided, will just save it for the next call.
                resultTracker.failure(currentError);
			}
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
			if (channel.isOpen()) {
                try {
                    channel.writeAndFlush(new MysqlQuitCommand());
                } finally {
				    channel.close().syncUninterruptibly();
                }
			}
            //NOTE: we don't shutdown the event loop. Under the shared thread model, we don't know if someone else is using it. -sgossard
			connectionEventGroup = null;
		}
	}

	@Override
	public void start(DevXid xid, CompletionHandle<Boolean> promise) {
		hasActiveTransaction = true;
        execute(new SQLCommand("XA START " + xid.getMysqlXid()), DBEmptyTextResultConsumer.INSTANCE, promise);
    }

	@Override
	public void end(DevXid xid, CompletionHandle<Boolean> promise) {
        execute(new SQLCommand("XA END " + xid.getMysqlXid()), DBEmptyTextResultConsumer.INSTANCE, promise);
    }

	@Override
	public void prepare(DevXid xid, CompletionHandle<Boolean> promise) {
        execute(new SQLCommand("XA PREPARE " + xid.getMysqlXid()), DBEmptyTextResultConsumer.INSTANCE, promise);
	}

	@Override
	public void commit(DevXid xid, boolean onePhase, CompletionHandle<Boolean> promise) {
		String sql = "XA COMMIT " + xid.getMysqlXid();
		if (onePhase)
			sql += " ONE PHASE";
        execute(new SQLCommand(sql), DBEmptyTextResultConsumer.INSTANCE, new DelegatingCompletionHandle<Boolean>(promise){
            @Override
            public void success(Boolean returnValue) {
                clearActiveState();
                super.success(returnValue);
            }
        });
	}
	@Override
	public void rollback(DevXid xid, CompletionHandle<Boolean> promise) {
        execute(new SQLCommand("XA ROLLBACK " + xid.getMysqlXid()), DBEmptyTextResultConsumer.INSTANCE, new DelegatingCompletionHandle<Boolean>(promise){
            @Override
            public void success(Boolean returnValue) {
                clearActiveState();
                super.success(returnValue);
            }

            @Override
            public void failure(Exception e) {
                if (e instanceof PESQLStateException && backendErrorImpliesNoOpenXA((PESQLStateException)e)){
                    logger.warn("tried to rollback transaction, but response implied no XA exists, " + e.getMessage());
                    clearActiveState();
                    super.success(true);
                } else {
                    super.failure(e);
                }
            }
        });
	}

    private void clearActiveState() {
        pendingUpdate = false;
        hasActiveTransaction = false;
    }

    public boolean backendErrorImpliesNoOpenXA(PESQLStateException mysqlError) {
        return mysqlError.getErrorNumber() == 1397 || //XAER_NOTA, XA id is completely unknown.
               (mysqlError.getErrorNumber() == 1399 && mysqlError.getErrorMsg().contains("NON-EXISTING")) //XAER_RMFAIL, because we are in NON-EXISTING state
        ;
    }

    @Override
    public void sendPreamble(String siteName, CompletionHandle<Boolean> promise) {
        execute(new SQLCommand("set @" + DBNative.DVE_SITENAME_VAR + "='" + siteName + "',character_set_connection='" + MysqlNativeConstants.DB_CHAR_SET
                + "', character_set_client='" + MysqlNativeConstants.DB_CHAR_SET + "', character_set_results='" + MysqlNativeConstants.DB_CHAR_SET + "'"),
                DBEmptyTextResultConsumer.INSTANCE, promise);
    }

    @Override
    public void setTimestamp(long referenceTime, CompletionHandle<Boolean> promise) {
        String setTimestampSQL = "SET TIMESTAMP=" + referenceTime + ";";
        execute(new SQLCommand(setTimestampSQL), DBEmptyTextResultConsumer.INSTANCE, promise);
    }

    @Override
	public void setCatalog(String databaseName, CompletionHandle<Boolean> promise) {
		execute(new SQLCommand("use " + databaseName), DBEmptyTextResultConsumer.INSTANCE, promise);
	}

    @Deprecated
	@Override
	public void cancel() {
        logger.warn("Cancelling running query via MysqlConnection.cancel() currently not supported.");
        //KILL QUERY needs to be sent on a different socket than the target query.  Sent on the same socket, it pends until the target query finishes, oops.  -sgossard
//		try {
//            execute(new SQLCommand("KILL QUERY " + authHandler.getThreadID()), MysqlKillResultDiscarder.INSTANCE, wrapHandler(null));
//        } catch (PESQLException e) {
//			logger.warn("Failed to cancel active query", e);
//		}
	}

    private CompletionHandle<Boolean> wrapHandler(final CompletionHandle<Boolean> target) {
        if (target != null)
            return target;  //caller supplied a handler to track success/failure.
        else {
            return deferredErrorHandle;
        }
    }

	@Override
	public void failure(Exception e) {
		pendingException = new PEException("Unhandled exception received on server housekeeping sql statement", e);
	}

	@Override
	public void success(Boolean returnValue) {
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

    private class DeferredErrorHandle extends PEDefaultPromise<Boolean> {
        //supply our own handler that will report failures on some call in the future.
        //TODO: since query results generally come back in order, it would be good for these handlers to signal the next provided handler, not some arbitrary execute in the future. -sgossard
        @Override
        public void success(Boolean returnValue) {
            MysqlConnection.this.success(returnValue);
        }

        @Override
        public void failure(Exception e) {
            MysqlConnection.this.failure(e);
        }

    }
}
