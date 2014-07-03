package com.tesora.dve.worker;

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

import com.tesora.dve.common.DBType;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.mysql.MysqlConnection;
import com.tesora.dve.db.mysql.SharedEventLoopHolder;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import io.netty.channel.EventLoopGroup;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class DirectConnectionCache {
    static Logger logger = Logger.getLogger(DirectConnectionCache.class);

    static boolean suppressConnectionCaching = Boolean.getBoolean("SingleConnection.suppressConnectionCaching") || Boolean.getBoolean("DirectConnectionCache.suppressConnectionCaching");

    //SMG: does this really need to be concurrent, or even exist?  We only have one impl, and we never swap it out, YAGNI.  -sgossard
    private static Map<DBType, DBConnection.Factory> connectionFactoryMap = new ConcurrentHashMap<DBType, DBConnection.Factory>() {
        private static final long serialVersionUID = 1L;
        {
            if (suppressConnectionCaching)
                logger.warn("connection caching is disabled");

            put(DBType.MYSQL, new MysqlConnection.Factory());
        }
    };

    static GenericKeyedObjectPool<DSCacheKey, CachedConnection> connectionCache =
            new GenericKeyedObjectPool<DSCacheKey, CachedConnection>(
                    new DSCacheEntryFactory(),
                    /* maxActive (per key) */ -1,
                    GenericObjectPool.WHEN_EXHAUSTED_BLOCK, /* maxWait */ 15000,
                    /* maxIdle */ 2500,
                    /* maxTotal */ -1,
                    /* minIdle */ 0,
                    /* tests */ false, false,
                    /* timeBetweenEvictionRunsMillis */ 1000,
                    /* numTestsPerEvictionRun */ 15000,
                    /* minEvictableIdleTimeMillis */ 2000,
                    /* testWhileIdle */ false
                    );

    public static CachedConnection checkoutDatasource(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site) throws PESQLException {
        return checkoutDatasource(null,auth,additionalConnInfo,site);
    }
    public static CachedConnection checkoutDatasource(EventLoopGroup preferredEventLoop, UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site) throws PESQLException {
        try{
            if (preferredEventLoop == null)
                preferredEventLoop = SharedEventLoopHolder.getLoop();

            DSCacheKey datasourceKey = new DirectConnectionCache.DSCacheKey(preferredEventLoop,auth, additionalConnInfo, site);

            CachedConnection cacheEntry;
            if (DirectConnectionCache.suppressConnectionCaching) {
                cacheEntry = DirectConnectionCache.connect(datasourceKey);
            } else {
                cacheEntry = DirectConnectionCache.connectionCache.borrowObject(datasourceKey);
            }

            return cacheEntry;
        } catch (NoSuchElementException e) {
            if (e.getMessage().matches(".*java.net.ConnectException.*"))
                throw new PECommunicationsException("Cannot connect to '" + site.getMasterUrl() + "' as user '" + auth.userid + "'", e);

            throw new PESQLException("Unable to connect to site '" + site.getName() + "' as user '" + auth.userid + "'", e);
        } catch (Exception e) {
            throw new PESQLException("Unable to connect to site '" + site.getName() + "' as user '" + auth.userid + "'", e);
        }
    }

    public static void returnDatasource(CachedConnection entry) throws PESQLException {
        if (DirectConnectionCache.suppressConnectionCaching || entry == null || entry.hasActiveTransaction()) {
            DirectConnectionCache.discardDatasource(entry);
        } else {
            try {
                DirectConnectionCache.DSCacheKey datasourceKey = entry.cacheKey;

                if (logger.isDebugEnabled())
                    logger.debug("returnDatasource(): suppressCache = " + DirectConnectionCache.suppressConnectionCaching);

                DirectConnectionCache.connectionCache.returnObject(datasourceKey, entry);
            } catch (Exception e) {
                throw new PESQLException(e);
            }
        }
    }

    public static void discardDatasource(CachedConnection entry) throws PESQLException {
        if (entry == null)
            return;

        if (logger.isDebugEnabled())
            logger.debug("discardDatasource()");

        entry.close();
    }

    public static void clearConnectionCache() {
        connectionCache.clear();
    }

    /**
     * @param key
     * @return
     * @throws java.sql.SQLException
     * @throws com.tesora.dve.exceptions.PEException
     */
    static CachedConnection connect(DSCacheKey key) throws SQLException, PEException {

        if (SingleDirectConnection.logger.isDebugEnabled())
            SingleDirectConnection.logger.debug("Allocating new JDBC connection to " + key.site.getName() + " ==> " + key.toString());

        PEUrl dbUrl = PEUrl.fromUrlString(key.url);
        DBType dbType = DBType.valueOf(dbUrl.getSubProtocol().toUpperCase());

        DBConnection dbConnection = connectionFactoryMap.get(dbType).newInstance(key.eventLoop,key.site);
        dbConnection.connect(key.url, key.userId, key.password, key.clientCapabilities);

        return new CachedConnection(key, dbConnection);
    }

    static class CachedConnection implements DBConnection {
        final DSCacheKey cacheKey;
        final DBConnection dbConnection;

        public CachedConnection(DSCacheKey cacheKey, DBConnection dbConnection) {
            this.cacheKey = cacheKey;
            this.dbConnection = dbConnection;
        }

        @Override
        public void connect(String url, String userid, String password,  long clientCapabilities) throws PEException {
            dbConnection.connect(url,userid,password, clientCapabilities);
        }

        @Override
        public void close() {
            dbConnection.close();
        }

        @Override
        public void execute(SQLCommand sql, DBResultConsumer consumer, CompletionHandle<Boolean> promise) {
            dbConnection.execute(sql,consumer,promise);
        }

        @Override
        public void start(DevXid xid, CompletionHandle<Boolean> promise) {
            dbConnection.start(xid, promise);
        }

        @Override
        public void end(DevXid xid, CompletionHandle<Boolean> promise) {
            dbConnection.end(xid, promise);
        }

        @Override
        public void prepare(DevXid xid, CompletionHandle<Boolean> promise) {
            dbConnection.prepare(xid, promise);
        }

        @Override
        public void commit(DevXid xid, boolean onePhase, CompletionHandle<Boolean> promise) {
            dbConnection.commit(xid,onePhase,promise);
        }

        @Override
        public void rollback(DevXid xid, CompletionHandle<Boolean> promise) {
            dbConnection.rollback(xid, promise);
        }

        @Override
        public void sendPreamble(String siteName, CompletionHandle<Boolean> promise)  {
            dbConnection.sendPreamble(siteName, promise);
        }

        @Override
        public void setCatalog(String databaseName, CompletionHandle<Boolean> promise) {
            dbConnection.setCatalog(databaseName, promise);
        }

        @Override
        public void setTimestamp(long referenceTime, CompletionHandle<Boolean> promise) {
            dbConnection.setTimestamp(referenceTime, promise);
        }

        @Deprecated
        @Override
        public void cancel() {
            dbConnection.cancel();
        }

        @Override
        public boolean hasPendingUpdate() {
            return dbConnection.hasPendingUpdate();
        }

        @Override
        public boolean hasActiveTransaction() {
            return dbConnection.hasActiveTransaction();
        }

        @Override
        public int getConnectionId() {
            return dbConnection.getConnectionId();
        }

        @Override
        public void success(Boolean returnValue) {
            dbConnection.success(returnValue);
        }

        @Override
        public void failure(Exception e) {
            dbConnection.failure(e);
        }

    }

    public static class DSCacheKey {
        final long CLIENT_CAPABILITIES_MASK = ClientCapabilities.DEFAULT_PSITE_CAPABILITIES | ClientCapabilities.CLIENT_FOUND_ROWS;
        private final EventLoopGroup eventLoop;
        private long clientCapabilities;
        private final String userId;
        private final String password;
        private final String url;
        private final boolean adminUser;
        private final StorageSite site;

        public DSCacheKey(EventLoopGroup eventLoop,UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site) {
            this.clientCapabilities = additionalConnInfo.getClientCapabilities() & CLIENT_CAPABILITIES_MASK;
            this.eventLoop = eventLoop;
            this.userId = auth.getUserid();
            this.password = auth.getPassword();
            this.url = site.getMasterUrl();
            this.adminUser = auth.isAdminUser();
            this.site = site;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + site.getName().hashCode();
            result = prime * result + ((url == null) ? 0 : url.hashCode());
            result = prime * result
                    + ((userId == null) ? 0 : userId.hashCode());
            result = prime * result + (int)clientCapabilities;
            result = prime * result + eventLoop.hashCode();
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DSCacheKey other = (DSCacheKey) obj;
            if (!site.getName().equals(other.site.getName()))
                return false;
            if (url == null) {
                if (other.url != null)
                    return false;
            } else if (!url.equals(other.url))
                return false;
            if (userId == null) {
                if (other.userId != null)
                    return false;
            } else if (!userId.equals(other.userId)){
                return false;
            } else if (clientCapabilities != other.clientCapabilities){
                return false;
            } else if (eventLoop != other.eventLoop)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return new StringBuffer().append("DSCacheKey(").append(userId).append(adminUser ? "[admin], " : ", ").append(url).append(",").append(eventLoop).append(")").toString();
        }
    }

    private static class DSCacheEntryFactory extends BaseKeyedPoolableObjectFactory<DSCacheKey, CachedConnection> {

        static AtomicInteger createCount = new AtomicInteger();
        static AtomicInteger activateCount = new AtomicInteger();
        static AtomicInteger destroyCount = new AtomicInteger();

        @Override
        public CachedConnection makeObject(DSCacheKey key) throws Exception {
            return connect(key);
        }

        @Override
        public void activateObject(DSCacheKey key, CachedConnection entry) throws Exception {
            if (SingleDirectConnection.logger.isDebugEnabled())
                SingleDirectConnection.logger.debug("Re-activating JDBC connection to " + key.site.getName() + " ==> " + key.toString());

            Singletons.require(HostService.class).getDBNative().postConnect(entry.dbConnection, key.site.getName());
        }

        @Override
        public void destroyObject(DSCacheKey key, CachedConnection obj)
                throws Exception {
            obj.close();
            super.destroyObject(key, obj);
        }

    }
}
