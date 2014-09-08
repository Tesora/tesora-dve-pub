package com.tesora.dve.mysqlapi.repl;

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

import com.tesora.dve.common.UserVisibleDatabase;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.mysql.DefaultResultProcessor;
import com.tesora.dve.db.mysql.MysqlConnection;
import com.tesora.dve.db.mysql.SynchronousResultProcessor;
import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.*;
import com.tesora.dve.mysqlapi.repl.messages.MyComBinLogDumpRequest;
import com.tesora.dve.mysqlapi.repl.messages.MyComRegisterSlaveRequest;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.AdditionalConnectionInfo;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.Worker;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.log4j.Logger;

import java.util.Map;

public class MyReplSlaveClientConnection {
    private static final Logger logger = Logger.getLogger(MyReplSlaveClientConnection.class);

    private final MyReplicationSlaveService plugin;
    private final MyClientConnectionContext context;

    private final DefaultResultProcessor logEventProcessor;

    protected boolean connected = false;
    private MysqlConnection mysqlConn = null;
    private EventLoopGroup workerGroup = null;

    enum State { WAITING_FOR_GREETING, AUTHENTICATING, REGISTERING_AS_SLAVE, PROCESSING_EVENTS }

    public MyReplSlaveClientConnection(MyReplSlaveConnectionContext rsContext, MyReplicationSlaveService plugin) {
        this(rsContext,plugin, new MyReplSlaveAsyncHandler(rsContext, plugin));
    }

    public MyReplSlaveClientConnection(MyReplSlaveConnectionContext rsContext, MyReplicationSlaveService plugin, DefaultResultProcessor processor) {
        this.context = rsContext;
        this.plugin = plugin;
        this.logEventProcessor = processor;
    }

    public boolean start() {
        boolean ret = false;

        workerGroup = new NioEventLoopGroup(1); //only use one thread for this socket.
        mysqlConn = new MysqlConnection(workerGroup, buildDummySite("replication"));

        try {
            String url = "jdbc:mysql://" + context.getConnectHost() + ":" + context.getConnectPort();
            long clientCapabilities = ClientCapabilities.CLIENT_LONG_PASSWORD
                    + ClientCapabilities.CLIENT_LONG_FLAG
                    + ClientCapabilities.CLIENT_LOCAL_FILES
                    + ClientCapabilities.CLIENT_PROTOCOL_41
                    + ClientCapabilities.CLIENT_TRANSACTIONS
                    + ClientCapabilities.CLIENT_SECURE_CONNECTION;
            mysqlConn.connect(url, plugin.getMasterUserid(), plugin.getMasterPwd(), clientCapabilities);

            //now authenticated, send slave ID registration.
            {
                MyComRegisterSlaveRequest rsr = new MyComRegisterSlaveRequest(plugin.getSlaveServerID(), plugin.getMasterPort());
                SynchronousResultProcessor registerProcessor = buildRegisterProcessor();
                mysqlConn.execute(rsr, registerProcessor);
                registerProcessor.sync();
            }

            //registered as slave OK, send dump binlog request.
            {
                MyBinLogPosition blp = plugin.getBinLogPosition();
                MyComBinLogDumpRequest blr = new MyComBinLogDumpRequest( plugin.getSlaveServerID(), blp.getPosition(), blp.getFileName());
                mysqlConn.execute(blr,logEventProcessor);
                //don't synchronize on a result here, a log dump isn't required to end.
            }

            //Authenticated, registered as slave, and sent dump binlog request, looks OK.
            this.connected = true;
            return true;
        } catch (Exception e) {
            logger.warn("Problem starting replication stream, ",e);
        }
        this.connected = false;
        return false;
    }

    private SynchronousResultProcessor buildRegisterProcessor() {
        return new SynchronousResultProcessor(){
            @Override
            public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
                if (message instanceof MyOKResponse) {
                    super.success(true);
                    return true;
                } else if (message instanceof MyErrorResponse){
                    MyErrorResponse err = (MyErrorResponse)message;
                    super.failure(err.asException());
                } else {
                    PEException unexpected = new PEException("Unexpected message received registering as slave, "+message);
                    unexpected.fillInStackTrace();
                    super.failure(unexpected);
                }
                return false;
            }
        };
    }

    public void stop() {
        try {
            mysqlConn.close();
        } catch (Exception e){
            logger.warn("Problem trying to close connection, ",e);
        }

        if (workerGroup != null) {
            if (!workerGroup.isShuttingDown()) {
                workerGroup.shutdownGracefully();
                workerGroup = null;
            }
        }
    }

    public boolean isStarted() {
        return (mysqlConn != null && mysqlConn.isActive());
    }

    public boolean isConnected() {
        if (!isStarted()) {
            return false;
        }
        return connected;
    }

    protected void setConnected(boolean connected){
        //really only used by testing infrastructure.
        this.connected = connected;
    }

    public MyClientConnectionContext getContext() {
        return context;
    }

    private StorageSite buildDummySite(final String description) {
        return new StorageSite() {
            @Override
            public String getName() {
                return description;
            }

            @Override
            public String getMasterUrl() {
                return null;
            }

            @Override
            public String getInstanceIdentifier() {
                return description;
            }

            @Override
            public PersistentSite getRecoverableSite(CatalogDAO c) {
                return null;
            }

            @Override
            public Worker pickWorker(Map<StorageSite, Worker> workerMap) throws PEException {
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
            public void setHasDatabase(UserVisibleDatabase ctxDB) throws PEException {

            }
        };
    }
}
