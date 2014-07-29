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

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.DelegatingCompletionHandle;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.db.mysql.SetVariableSQLBuilder;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.worker.agent.Agent;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.ISiteInstance;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.resultset.collector.ResultChunkManager;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;

/**
 * Worker is the agent that processes commands against the database. It
 * maintains a JDBC connection to a particular database (either Storage or
 * Temp), though which database it is connected to can be changed by the
 * WorkerManager.
 */
public abstract class Worker implements GenericSQLCommand.DBNameResolver {
	
	public interface Factory {
		public Worker newWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop) throws PEException;

		public void onSiteFailure(StorageSite site) throws PEException;

		public String getInstanceIdentifier(StorageSite site, ISiteInstance instance);
	}
	
	// TODO: With XADataSource, we are now using a connection pool. Does it
	// still make sense to
	// have the worker keep connections, or should we get a new one for each
	// transaction and
	// allow the connection pool to manage them?

	private static Logger logger = Logger.getLogger(Worker.class);
	
	private static AtomicLong nextWorkerId = new AtomicLong();

    EventLoopGroup previousEventLoop;
    EventLoopGroup preferredEventLoop;

	StorageSite site;
	UserAuthentication userAuthentication;
	AdditionalConnectionInfo additionalConnInfo;
	
	@Override
	public String toString() {
		return getName()+"("+site+")";
	}

	WorkerConnection wConnection = null;
	boolean connectionAllocated = false;

	ResultChunkManager chunkMgr;
	String currentDatabaseName = null;
	String userVisibleDatabaseName;
	Integer currentDatabaseID = null;
	Integer previousDatabaseID = null;
	String currentGlobalTransaction = null;

	final String name;
	
	Exception lastException = null;

	long lastAccessTime = System.currentTimeMillis();

    boolean bindingChangedSinceLastCatalogSet = false;

	Worker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop) throws PEException {
		this.name = this.getClass().getSimpleName() + nextWorkerId.incrementAndGet();
		this.site = site;
		this.userAuthentication = auth;
		this.additionalConnInfo = additionalConnInfo;
        this.previousEventLoop = null;
        this.preferredEventLoop = preferredEventLoop;
	}

	public abstract WorkerConnection getConnection(StorageSite site, AdditionalConnectionInfo additionalConnInfo, UserAuthentication auth, EventLoopGroup preferredEventLoop);

    public void bindToClientThread(EventLoopGroup eventLoop) throws PESQLException {
        this.previousEventLoop = this.preferredEventLoop;
        this.preferredEventLoop = eventLoop;
        if (wConnection != null){
            wConnection.bindToClientThread(eventLoop);
            bindingChangedSinceLastCatalogSet = true;
        }
    }

	void sendStatistics(WorkerRequest wReq, long execTime) throws PEException {
		LogSiteStatisticRequest sNotice = wReq.getStatisticsNotice();
		if (sNotice != null) {
			site.annotateStatistics(sNotice);
			sNotice.setExecutionDetails(site.getName(), (int)execTime);
            Agent.dispatch(Singletons.require(HostService.class).getStatisticsManagerAddress(), sNotice);
		}
	}

	public void releaseResources() throws PEException {
//		try {
//			resetStatement();
			closeWConnection();
//		} catch (SQLException e1) {
//			if (logger.isDebugEnabled())
//				throw new PEException("Exception encountered releasing resources for worker " + getName(), e1);
//		}
	}

	private void closeWConnection() {
		if (wConnection != null) {
			try {
                PEDefaultPromise<Boolean> promise = new PEDefaultPromise<>();
				resetStatement(promise);
                promise.sync();
//				rollback(currentGlobalTransaction);
				wConnection.close(lastException == null);
			} catch (Exception e) {
                try {
                    wConnection.close(false);
                } catch (PESQLException e1) {
                    logger.warn("Encountered problem resetting and closing connection "+wConnection, e);
                }
            }
			wConnection = null;
		}
	}

	public void resetStatement(CompletionHandle<Boolean> promise) throws SQLException {

        CompletionHandle<Boolean> resultTracker = new DelegatingCompletionHandle<Boolean>(promise){
            @Override
            public void success(Boolean returnValue) {
                if (chunkMgr != null) {
                    try {
                        chunkMgr.close();
                        chunkMgr = null;
                    } catch (SQLException e) {
                        this.failure(e);
                        return;
                    }
                }
                super.success(returnValue);
            }

            @Override
            public void failure(Exception e) {
                if (e instanceof SQLException)
                    super.failure(e);

                SQLException problem = new SQLException("Unable to reset worker connection", e);
                problem.fillInStackTrace();
                super.failure(problem);
            }

        };
		if (!StringUtils.isEmpty(currentGlobalTransaction)) {
			try {
				rollback(currentGlobalTransaction, resultTracker);
//				currentGlobalTransaction = null;
			} catch (Exception e) {
				SQLException problem = new SQLException("Unable to reset worker connection", e);
                problem.fillInStackTrace();
                promise.failure(problem);
                return;
			}
        } else {
            promise.success(true);
        }
	}

	public void setCurrentDatabase(PersistentDatabase currentDatabase) throws PEException {
		if (currentDatabase != null) {
			// the physical db name might be the same, but we could have dropped the physical db in between
			// check to make sure that the udb id has not changed
			String newDatabaseName = currentDatabase.getNameOnSite(site);

			if (bindingChangedSinceLastCatalogSet || !newDatabaseName.equals(currentDatabaseName) || (currentDatabaseID != null && currentDatabaseID.intValue() != currentDatabase.getId()) ) {
                previousDatabaseID = currentDatabaseID;
				currentDatabaseID = currentDatabase.getId();
				currentDatabaseName = newDatabaseName;
				userVisibleDatabaseName = currentDatabase.getUserVisibleName();
                previousEventLoop = preferredEventLoop;
				getConnection().setCatalog(currentDatabaseName);
                bindingChangedSinceLastCatalogSet = false;
			}
		}
	}

    public void updateSessionVariables(Map<String,String> desiredVariables, SetVariableSQLBuilder setBuilder, CompletionHandle<Boolean> promise) {
        WorkerConnection connection = getConnection();
        connection.updateSessionVariables(desiredVariables, setBuilder, promise);
    }
	
	// used in late resolution support
	@Override
	public String getNameOnSite(String dbName) {
		return UserDatabase.getNameOnSite(dbName, site);
	}
	
	@Override
	public int getSiteIndex() {
		return site.getInstanceIdentifier().hashCode();
	}

	public boolean databaseChanged() {
		return !ObjectUtils.equals(previousDatabaseID, currentDatabaseID);
	}
	
	public void setPreviousDatabaseWithCurrent() {
		previousDatabaseID = currentDatabaseID;
	}
	
	private WorkerConnection getConnection() {
		if (wConnection == null) {
			if (connectionAllocated)
				throw new PECodingException("Worker connection reallocated");
			wConnection = getConnection(site, additionalConnInfo, userAuthentication, preferredEventLoop);
			connectionAllocated = true;
		}
		return wConnection;
	}

	public void setChunkManager(ResultChunkManager mgr) {
		chunkMgr = mgr;
	}

	public ResultChunkManager getChunkManager() {
		return chunkMgr;
	}

	String getWorkerId() throws PEException {
		return getName();
	}
	
	public String getName() {
		return name;
	}

	public StorageSite getWorkerSite() {
		return site;
	}

	public void setSite(StorageSite site2) {
		this.site = site2;
	}

	public DevXid getXid(String globalId) {
		return new DevXid(globalId, getName());
	}

	public void startTrans(String globalId) throws PEException  {
		if (site.supportsTransactions()) {
			if (StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".startTrans("+globalId+")");
				getConnection().startXA(getXid(globalId));
				currentGlobalTransaction = globalId;
			} else if (currentGlobalTransaction != globalId)
				throw new PEException("Transaction id mismatch (expected " + currentGlobalTransaction
						+ ", got " + globalId + ")");
		}
	}

	public void endTrans(String globalId) throws PEException {
		if (site.supportsTransactions()) {
			if (!StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".endTrans("+globalId+")");
				if (currentGlobalTransaction != globalId)
					throw new PEException("Transaction id mismatch (expected "
							+ currentGlobalTransaction + ", got " + globalId + ")");
				getConnection().endXA(getXid(globalId));
			}
		}
	}

	public void prepare(String globalId, CompletionHandle<Boolean> promise) {
		if (site.supportsTransactions()) {
			if (!StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".prepare("+globalId+")");
				if (currentGlobalTransaction != globalId) {
					PEException problem = new PEException("Transaction id mismatch (expected "
							+ currentGlobalTransaction + ", got " + globalId + ")");
                    problem.fillInStackTrace();
                    promise.failure(problem);
                    return;
                }
                try {
                    endTrans(globalId);
                } catch (PEException e) {
                    promise.failure(e);
                    return;
                }

                getConnection().prepareXA(getXid(globalId), promise);
                return;
			}
		}

        promise.success(true);
	}

	public void commit(String globalId, boolean onePhase, CompletionHandle<Boolean> promise) {
		if (site.supportsTransactions()) {
			if (!StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".commit("+globalId+")");
				if (currentGlobalTransaction != globalId) {
					PEException problem = new PEException("Transaction id mismatch (expected "
							+ currentGlobalTransaction + ", got " + globalId + ")");
                    problem.fillInStackTrace();
                    promise.failure(problem);
                    return;
                }

                try {

                    if (onePhase)
                        endTrans(globalId);

                } catch (PEException e) {
                    promise.failure(e);
                    return;
                }

                CompletionHandle<Boolean> resultTracker = new DelegatingCompletionHandle<Boolean>(promise){
                    @Override
                    public void success(Boolean returnValue) {
                        currentGlobalTransaction = null;
                        super.success(returnValue);
                    }

                };

                getConnection().commitXA(getXid(globalId), onePhase, resultTracker);
                return;
			}
		}
        promise.success(true);
	}

	public void rollback(String globalId, CompletionHandle<Boolean> promise) {
		if (site.supportsTransactions()) {
			if ( /* isModified() && */ !StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".rollback("+globalId+")");
				if (currentGlobalTransaction != globalId){
                    PEException problem = new PEException("Transaction id mismatch (expected "
							+ currentGlobalTransaction + ", got " + globalId + ")");
                    problem.fillInStackTrace();
                    promise.failure(problem);
                    return;
                }

                try {
                    endTrans(globalId);
                } catch (PEException e) {
                    promise.failure(e);
                    return;
                }

                currentGlobalTransaction = null;
				getConnection().rollbackXA(getXid(globalId), promise);
			}
		}

        promise.success(true);
	}

	public void close() throws PEException {
		try {
			releaseResources();
		} catch (PEException e) {
			logger.warn("Exception encountered closing worker " + getName(), e);
		}
	}

	public long getLastAccessTime() {
		return lastAccessTime;
	}

	public String getCurrentDatabaseName() {
		return currentDatabaseName;
	}
	
	public String getUserVisibleDatabaseName() {
		return userVisibleDatabaseName;
	}

	public WorkerStatement getStatement() throws PESQLException {
		WorkerStatement workerStatement;
		
		try {
			workerStatement = getConnection().getStatement(this);
		} catch (PECommunicationsException ce) {
			onCommunicationsFailure();
			throw ce;
		}
		
		return workerStatement;
	}

	public void onCommunicationsFailure() throws PESQLException {
		closeWConnection();
//		lastException = PECommunicationsException.INSTANCE;
		try {
            Agent.dispatch(Singletons.require(HostService.class).getNotificationManagerAddress(), new SiteFailureNotification(site));
		} catch (PEException e1) {
			logger.error("Unable to send site failure notification", e1);
		}
	}
	
	private int uniqueValue = 0;

	private Future<Void> executionFuture;

	public void setLastException(Exception lastException) {
		this.lastException = lastException;
	}

	public long getUniqueValue() {
		return ++uniqueValue  ;
	}

	public boolean isActiveSiteInstance(int siteInstanceId) {
		return site.getMasterInstanceId() == siteInstanceId;
	}

	public void setFuture(Future<Void> future) {
		this.executionFuture = future;
	}

	public Future<Void> getFuture() {
		return executionFuture;
	}

	public boolean isModified() throws PESQLException {
		return getConnection().isModified();
	}
	
	public String getAddress() {
		return name;
	}

	public boolean hasActiveTransaction() throws PESQLException {
		return getConnection().hasActiveTransaction();
	}

	public int getConnectionId() throws PESQLException {
		return getConnection().getConnectionId();
	}
}
