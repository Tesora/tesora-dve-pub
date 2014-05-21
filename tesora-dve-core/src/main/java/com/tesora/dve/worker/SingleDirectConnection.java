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

import java.sql.SQLException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import com.tesora.dve.common.DBType;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.DBConnection.Factory;
import com.tesora.dve.db.mysql.MysqlConnection;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.exceptions.PESQLQueryInterruptedException;

public class SingleDirectConnection implements WorkerConnection {
	
	static Logger logger = Logger.getLogger(SingleDirectConnection.class);
	
	static boolean suppressConnectionCaching = Boolean.getBoolean("SingleConnection.suppressConnectionCaching");
	
	static Map<DBType, Factory> connectionFactoryMap = new ConcurrentHashMap<DBType, DBConnection.Factory>() {
		private static final long serialVersionUID = 1L;
		{
			if (suppressConnectionCaching)
				logger.warn(SingleDirectConnection.class.getSimpleName() + " caching is disabled");
			
			put(DBType.MYSQL, new MysqlConnection.Factory());
		}
	};

	static class DSCacheEntry {
		DBConnection dbConnection;
		public DSCacheEntry(DBConnection dbConnection) {
			this.dbConnection = dbConnection;
		}
		/**
		 * @throws PESQLException
		 */
		public void close() throws PESQLException {
			dbConnection.close();
		}
	}
	
	static class DSCacheKey {
		String userId;
		String password;
		String url;
		boolean adminUser;
		StorageSite site;
		DSCacheKey(UserAuthentication auth, StorageSite site) {
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
			} else if (!userId.equals(other.userId))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return new StringBuffer().append("DSCacheKey(").append(userId).append(adminUser ? "[admin], " : ", ").append(url).append(")").toString();
		}
	}
	
	static class DSCacheEntryFactory extends BaseKeyedPoolableObjectFactory<DSCacheKey, DSCacheEntry> {
		
		static AtomicInteger createCount = new AtomicInteger();
		static AtomicInteger activateCount = new AtomicInteger();
		static AtomicInteger destroyCount = new AtomicInteger();

		@Override
		public DSCacheEntry makeObject(DSCacheKey key) throws Exception {
			return getCacheEntry(key);
		}

		@Override
		public void activateObject(DSCacheKey key, DSCacheEntry entry) throws Exception {
			if (logger.isDebugEnabled())
				logger.debug("Re-activating JDBC connection to " + key.site.getName() + " ==> " + key.toString());

            Singletons.require(HostService.class).getDBNative().postConnect(entry.dbConnection, key.site.getName());
		}

		@Override
		public void destroyObject(DSCacheKey key, DSCacheEntry obj)
				throws Exception {
			obj.close();
			super.destroyObject(key, obj);
		}
		
	}
	
	static GenericKeyedObjectPool<DSCacheKey, DSCacheEntry> connectionCache = 
			new GenericKeyedObjectPool<SingleDirectConnection.DSCacheKey, SingleDirectConnection.DSCacheEntry>(
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

	/**
	 * @param key
	 * @return
	 * @throws SQLException
	 * @throws PEException
	 */
	static DSCacheEntry getCacheEntry(DSCacheKey key) throws SQLException, PEException {

		if (logger.isDebugEnabled())
			logger.debug("Allocating new JDBC connection to " + key.site.getName() + " ==> " + key.toString());

		PEUrl dbUrl = PEUrl.fromUrlString(key.url);
		DBType dbType = DBType.valueOf(dbUrl.getSubProtocol().toUpperCase());
		
		DBConnection dbConnection = connectionFactoryMap.get(dbType).newInstance(key.site);
		dbConnection.connect(key.url, key.userId, key.password);

		return new DSCacheEntry(dbConnection);
	}

	final UserAuthentication userAuthentication;
	final StorageSite site;

	AtomicReference<DSCacheEntry> datasourceInfo = new AtomicReference<SingleDirectConnection.DSCacheEntry>();
	
	WorkerStatement wSingleStatement = null;

	private DSCacheKey datasourceKey;

	public SingleDirectConnection(final UserAuthentication auth, final StorageSite site) {
		this.userAuthentication = auth;
		this.site = site;
	}

	@Override
	public WorkerStatement getStatement(Worker w) throws PESQLException {
		if (wSingleStatement==null) {
			w.setPreviousDatabaseWithCurrent();
				
			wSingleStatement = getNewStatement(w);
		}
		return wSingleStatement;
	}

	/**
	 * @param w
	 * @throws PESQLException
	 */
	protected void onCommunicationsFailure(Worker w) throws PESQLException {
		// do nothing?
	}

	protected SingleDirectStatement getNewStatement(Worker w) throws PESQLException {
		return new SingleDirectStatement(w, getConnection());
	}

	
	DSCacheEntry getDataSourceInfo() throws PESQLException {
		DSCacheEntry cacheEntry = datasourceInfo.get();
		if (cacheEntry == null) {
			try {
				synchronized (this) {
					datasourceKey = new DSCacheKey(userAuthentication, site);
				}
				if (suppressConnectionCaching) {
					cacheEntry = getCacheEntry(datasourceKey);
				} else {
					cacheEntry = connectionCache.borrowObject(datasourceKey);
				}
				if (!datasourceInfo.compareAndSet(null, cacheEntry))
					cacheEntry = datasourceInfo.get();
			} catch (NoSuchElementException e) {
				if (e.getMessage().matches(".*java.net.ConnectException.*"))
					throw new PECommunicationsException("Cannot connect to '" + site.getMasterUrl() + "' as user '" + userAuthentication.userid + "'", e);

				throw new PESQLException("Unable to connect to site '" + site.getName() + "' as user '" + userAuthentication.userid + "'", e);
			} catch (Exception e) {
				throw new PESQLException("Unable to connect to site '" + site.getName() + "' as user '" + userAuthentication.userid + "'", e);
			}
		}
		return cacheEntry;
	}

	protected DBConnection getConnection() throws PESQLException {
		return getDataSourceInfo().dbConnection;
	}
	
	@Override
	public synchronized void close(boolean isStateValid) throws PESQLException {
		closeActiveStatements();

		if (datasourceInfo.get() != null) {
			try {
				if (logger.isDebugEnabled())
					logger.debug("SingleConnection.close(): isStateValid = " + isStateValid + ", suppressCache = " + suppressConnectionCaching);

				if (!isStateValid || getConnection().hasActiveTransaction() || suppressConnectionCaching) {
					datasourceInfo.get().close();
				} else {
					connectionCache.returnObject(datasourceKey, datasourceInfo.get());
				}
			} catch (Exception e) {
				throw new PESQLException(e);
			}
			datasourceInfo.set(null);
		}
	}

	@Override
	public void closeActiveStatements() throws PESQLException {
		if (wSingleStatement != null) {
			wSingleStatement.close();
			wSingleStatement = null;
		}
	}

	@Override
	public void setCatalog(String databaseName) throws PESQLException {
		try {
			getConnection().setCatalog(databaseName);
		} catch (PESQLQueryInterruptedException e) {
			throw e;
		} catch (Exception e) {
			throw new PESQLException("Unhandled exception setting database " + databaseName, e);
		}
	}

	@Override
	public void rollbackXA(DevXid xid) throws PESQLException {
		try {
			getConnection().rollback(xid);
		} catch (Exception e) {
			throw new PESQLException("Cannot rollback XA Transaction " + xid, e);
		}
	}

	@Override
	public void commitXA(DevXid xid, boolean onePhase) throws PESQLException {
		try {
			getConnection().commit(xid, onePhase);
		} catch (Exception e) {
			throw new PESQLException("Cannot commit XA Transaction " + xid, e);
		}
	}

	@Override
	public void prepareXA(DevXid xid) throws PESQLException {
		try {
			getConnection().prepare(xid);
		} catch (Exception e) {
			throw new PESQLException("Cannot prepare XA Transaction " + xid, e);
		}
	}

	@Override
	public void endXA(DevXid xid) throws PESQLException {
		try {
			getConnection().end(xid);
		} catch (Exception e) {
			throw new PESQLException("Cannot end XA Transaction " + xid, e);
		}
	}

	@Override
	public void startXA(DevXid xid) throws PESQLException {
		try {
			getConnection().start(xid);
		} catch (Exception e) {
			throw new PESQLException("Cannot start XA Transaction " + xid, e);
		}
	}


	@Override
	public boolean isModified() throws PESQLException {
		return getConnection().hasPendingUpdate();
	}
	
	public static void clearConnectionCache() {
		connectionCache.clear();
	}

	@Override
	public boolean hasActiveTransaction() throws PESQLException {
		return getConnection().hasActiveTransaction();
	}

	@Override
	public int getConnectionId() throws PESQLException {
		return getConnection().getConnectionId();
	}

}
