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
import com.tesora.dve.db.*;
import com.tesora.dve.db.mysql.libmy.MyMessage;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Objects;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.DevXid;
import com.tesora.dve.worker.UserCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlConnection implements DBConnection, DBConnection.Monitor, CommandChannel {

	public static final boolean USE_POOLED_BUFFERS = Boolean.getBoolean("com.tesora.dve.netty.usePooledBuffers");

	private static final boolean PACKET_LOGGER = Boolean.getBoolean("MysqlConnection.packetLogger");

	// TODO what's the general strategy for mapping MySQL error codes? (see PE-5)
	private static final int MYSQL_ERR_QUERY_INTERRUPTED = 1317;

	static Logger logger = LoggerFactory.getLogger(MysqlConnection.class);

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

    Map<String,String> currentSessionVariables = new HashMap<>();
    Map<String,String> sessionDefaults = new HashMap<>();

	public MysqlConnection(EventLoopGroup eventLoop, StorageSite site) {
        this.connectionEventGroup = eventLoop;
		this.site = site;
        setupDefaultSessionVars(site);
	}

    public MysqlConnection(StorageSite site) {
        this(SharedEventLoopHolder.getLoop(),site);
    }

    private void setupDefaultSessionVars(StorageSite site) {
        //provide some default session variables that can be overridden.
        sessionDefaults.put("@" + DBNative.DVE_SITENAME_VAR,site.getName());
        sessionDefaults.put("character_set_connection", MysqlNativeConstants.DB_CHAR_SET);
        sessionDefaults.put("character_set_client", MysqlNativeConstants.DB_CHAR_SET);
        sessionDefaults.put("character_set_results", MysqlNativeConstants.DB_CHAR_SET);
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
                        .addLast(MyBackendDecoder.class.getSimpleName(), new MyBackendDecoder(site.getName(), charsetHelper))
                        .addLast(StreamValve.class.getSimpleName(), new StreamValve())
                        .addLast(MysqlCommandSenderHandler.class.getSimpleName(), new MysqlCommandSenderHandler(site,MysqlConnection.this));
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

    public void write(MysqlCommand command){
        sendCommand(command);
    }

    public void writeAndFlush(MysqlCommand command){
        sendCommand(command);
    }

    /**
     * Currently the main entrypoint for dispatching old-style requests.  The MysqlCommand object holds both the
     * request and response, and writing the request and reading/dispatching the response is handled in the pipeline.
     * @param command
     */
    protected void sendCommand(MysqlCommand command){
        channel.writeAndFlush(command);
    }

    public String getName() {
        return site.getName();
    }

    @Override
    public StorageSite getStorageSite() {
        return site;
    }

    @Override
    public Monitor getMonitor() {
        return this;
    }

    public CompletionHandle<Boolean> getExceptionDeferringPromise() {
        return deferredErrorHandle;
    }

    public Exception getAndClearPendingException(){
        Exception currentError = pendingException;
        pendingException = null;
        return currentError;
    }

    public boolean isOpen() {
        return channel.isOpen();
    }

    //syntactic sugar for some of the inner utility calls.
    public void execute(String sql, CompletionHandle<Boolean> promise){
        this.execute( new SQLCommand(sql), promise);
    }

    //syntactic sugar for some of the inner utility calls.
    public void execute(SQLCommand sql, CompletionHandle<Boolean> promise){
        DBEmptyTextResultConsumer.INSTANCE.dispatch(this, sql, promise);
    }

    /**
     *  The main entrypoint for newer style calls that are just a protocol request and response processor.  Currently this
     *  wraps the request/response in the appropriate old-style objects.  When all methods operate through this method,
     *  it will be possible to clean up the pipeline and make it use the new types natively.
     * @param outboundMessage
     * @param resultsProcessor
     */
    @Override
    public void execute(MyMessage outboundMessage, DefaultResultProcessor resultsProcessor){
        buildDefaultConsumer(new SimpleMysqlCommand(outboundMessage, resultsProcessor)).dispatch(this, SQLCommand.EMPTY, resultsProcessor);
    }

	private void syncToServerConnect() {
		if (pendingConnection != null) {
			pendingConnection.syncUninterruptibly();
			pendingConnection = null;
		}
	}

    public boolean isActive(){
        return (channel != null && channel.isActive());
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
        execute("XA START " + xid.getMysqlXid(), promise);
    }

	@Override
	public void end(DevXid xid, CompletionHandle<Boolean> promise) {
        execute("XA END " + xid.getMysqlXid(), promise);
    }

	@Override
	public void prepare(DevXid xid, CompletionHandle<Boolean> promise) {
        execute("XA PREPARE " + xid.getMysqlXid(),promise);
	}

	@Override
	public void commit(DevXid xid, boolean onePhase, CompletionHandle<Boolean> promise) {
		String sql = "XA COMMIT " + xid.getMysqlXid();
		if (onePhase)
			sql += " ONE PHASE";
        execute(sql,new DelegatingCompletionHandle<Boolean>(promise){
            @Override
            public void success(Boolean returnValue) {
                clearActiveState();
                super.success(returnValue);
            }
        });
	}
	@Override
	public void rollback(DevXid xid, CompletionHandle<Boolean> promise) {
        execute("XA ROLLBACK " + xid.getMysqlXid(),new DelegatingCompletionHandle<Boolean>(promise){
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

	private static final AtomicLong updates = new AtomicLong(0);
	
    public void updateSessionVariables(Map<String,String> desiredVariables, SetVariableSQLBuilder setBuilder, CompletionHandle<Boolean> promise){
        final SQLCommand updateCommand;
        final Map<String,String> updatesRequired = new HashMap<>();
        try {

            Set<String> mergedKeys = new HashSet<>( desiredVariables.keySet() );
            mergedKeys.addAll( currentSessionVariables.keySet() );
            mergedKeys.addAll( sessionDefaults.keySet() );

            for (String variableName : mergedKeys){
                String existingValue = currentSessionVariables.get(variableName);
                String desiredValue;
                if (desiredVariables.containsKey(variableName))
                    desiredValue = desiredVariables.get(variableName);
                else
                    desiredValue = sessionDefaults.get(variableName);
                if (desiredValue == null){
                    updatesRequired.put(variableName,null);
                    setBuilder.remove(variableName,existingValue);
                } else if (existingValue == null) {
                    updatesRequired.put(variableName,desiredValue);
                    setBuilder.add(variableName, desiredValue);
                } else if ( ! desiredValue.equals( existingValue ) ) {
                    updatesRequired.put(variableName,desiredValue);
                    setBuilder.update(variableName,existingValue,desiredValue);
                } else
                    setBuilder.same(variableName,desiredValue);
            }

            updateCommand = setBuilder.generateSql();
//            System.out.println(updates.getAndIncrement() + ": " + updateCommand);
        } catch (Exception e) {
            callbackSetVariablesFailed(updatesRequired, null, e);
            promise.failure(e);
            return;
        }

        if (updateCommand == SQLCommand.EMPTY){
            promise.success(true);
        } else {
            execute(updateCommand, new DelegatingCompletionHandle<Boolean>( promise ){
                @Override
                public void success(Boolean returnValue) {
                    callbackSetVariablesOK(updatesRequired, updateCommand);
                    super.success(returnValue);
                }

                @Override
                public void failure(Exception e) {
                    callbackSetVariablesFailed(updatesRequired, updateCommand, e);
                    super.failure(e);
                }
            });
        }
    }

    private void callbackSetVariablesFailed(Map<String, String> updatesRequired, SQLCommand updateCommand, Exception e) {
        currentSessionVariables.clear();//not sure if this is ideal, but it forces variables to get set next attempt.
        logger.warn("Problem updating session variables on connection {} via command {}",new Object[]{site,updateCommand,e});
    }

    private void callbackSetVariablesOK(Map<String, String> updatesRequired, SQLCommand updateCommand) {
        for (Map.Entry<String,String> updateEntry : updatesRequired.entrySet()){
            String key = updateEntry.getKey();
            String value = updateEntry.getValue();
            if (value == null)
                currentSessionVariables.remove(key);
            else
                currentSessionVariables.put(key, value);
        }
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
    public void setTimestamp(long referenceTime, CompletionHandle<Boolean> promise) {
        String setTimestampSQL = "SET TIMESTAMP=" + referenceTime + ";";
        execute(setTimestampSQL, promise);
    }

    @Override
	public void setCatalog(String databaseName, CompletionHandle<Boolean> promise) {
		execute("use " + databaseName, promise);
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

	public void handleFailure(Exception e) {
		pendingException = new PEException("Unhandled exception received on server housekeeping sql statement", e);
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

    private static DBResultConsumer buildDefaultConsumer(final MysqlCommand command) {
        return new DBEmptyTextResultConsumer() {

            @Override
            public void writeCommandExecutor(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
                channel.write(command);
            }

        };
    }

    private class DeferredErrorHandle extends PEDefaultPromise<Boolean> {
        //supply our own handler that will report failures on some call in the future.
        //NOTE: we only use this when the caller didn't provide a promise, so it is OK that we don't call the parent success/failure, no one should ever be waiting. -sgossard

        //TODO: since query results generally come back in order, it would be good for these handlers to signal the next provided handler, not some arbitrary execute in the future. -sgossard
        @Override
        public void success(Boolean returnValue) {
            //ignore, no one is waiting.
        }

        @Override
        public void failure(Exception e) {
            MysqlConnection.this.handleFailure(e);
        }

    }


}
