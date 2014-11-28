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

import com.tesora.dve.concurrent.SynchronousListener;
import com.tesora.dve.db.CommandChannel;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.mysql.DelegatingResultsProcessor;
import com.tesora.dve.db.mysql.MysqlMessage;
import com.tesora.dve.db.mysql.SharedEventLoopHolder;
import io.netty.channel.EventLoopGroup;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.DelegatingCompletionHandle;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.db.mysql.SetVariableSQLBuilder;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.worker.agent.Agent;

/**
 * Worker is the agent that processes commands against the database. It
 * maintains a JDBC connection to a particular database (either Storage or
 * Temp), though which database it is connected to can be changed by the
 * WorkerManager.
 */
public class Worker implements GenericSQLCommand.DBNameResolver {
    static final EventLoopGroup DEFAULT_EVENTLOOP = SharedEventLoopHolder.getLoop();
    	
    public enum AvailabilityType { SINGLE, MASTER_MASTER }

    // TODO: With XADataSource, we are now using a connection pool. Does it
	// still make sense to
	// have the worker keep connections, or should we get a new one for each
	// transaction and
	// allow the connection pool to manage them?

	private static Logger logger = Logger.getLogger(Worker.class);
	
	private static AtomicLong nextWorkerId = new AtomicLong();

    EventLoopGroup previousEventLoop;
    EventLoopGroup preferredEventLoop;

	protected StorageSite site;
	protected UserAuthentication userAuthentication;
	protected AdditionalConnectionInfo additionalConnInfo;
	
	@Override
	public String toString() {
		return getName()+"("+site+")";
	}

    Object wConnection = null;
	boolean connectionAllocated = false;

	String currentDatabaseName = null;
	Integer currentDatabaseID = null;
	Integer previousDatabaseID = null;
	String currentGlobalTransaction = null;

	final String name;
	
	Exception lastException = null;

    boolean bindingChangedSinceLastCatalogSet = false;
    boolean statementFailureTriggersCommFailure;

    AtomicReference<DirectConnectionCache.CachedConnection> datasourceInfo = new AtomicReference<>();
    SingleDirectStatement wSingleStatement = null;

    protected Worker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop, AvailabilityType availability) {
        this.name = this.getClass().getSimpleName() + nextWorkerId.incrementAndGet();
        this.site = site;
        this.userAuthentication = auth;
        this.additionalConnInfo = additionalConnInfo;
        this.previousEventLoop = null;
        this.preferredEventLoop = preferredEventLoop;
        this.statementFailureTriggersCommFailure = (availability == AvailabilityType.MASTER_MASTER);
    }

    public void bindToClientThread(EventLoopGroup eventLoop) throws PESQLException {
        this.previousEventLoop = this.preferredEventLoop;
        this.preferredEventLoop = eventLoop;
        if (eventLoop == null)
            this.preferredEventLoop = DEFAULT_EVENTLOOP;

        if (wConnection != null){
            if (previousEventLoop != preferredEventLoop)
                dircon_releaseConnection(true);
            bindingChangedSinceLastCatalogSet = true;
        }
    }

    private void closeWConnection() {
		if (wConnection != null) {
			try {
                PEDefaultPromise<Boolean> promise = new PEDefaultPromise<>();
				resetStatement(promise);
                SynchronousListener.sync(promise);
//				rollback(currentGlobalTransaction);
                dircon_releaseConnection(lastException == null);
            } catch (Exception e) {
                try {
                    dircon_releaseConnection(false);
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
                previousEventLoop = preferredEventLoop;
                ensureConnection();
                dircon_getConnection().setCatalog(currentDatabaseName, null);
                bindingChangedSinceLastCatalogSet = false;
			}
		}
	}

    public void updateSessionVariables(Map<String,String> desiredVariables, SetVariableSQLBuilder setBuilder, CompletionHandle<Boolean> promise) {
        ensureConnection();
        DBConnection connection = null;
        try {
            connection = dircon_getConnection();
            connection.updateSessionVariables(desiredVariables, setBuilder, promise);
        } catch (PESQLException e) {
            promise.failure(e);
        }
    }

	// used in late resolution support
	public String getNameOnSite(String dbName) {
		return UserDatabase.getNameOnSite(dbName, site);
	}

	@Override
	public int getSiteIndex() {
		return site.getInstanceIdentifier().hashCode();
	}

	public void setPreviousDatabaseWithCurrent() {
		previousDatabaseID = currentDatabaseID;
	}

    private void ensureConnection() {
        if (wConnection == null) {
            if (connectionAllocated)
                throw new PECodingException("Worker connection reallocated");
            wConnection = new Object();
            connectionAllocated = true;
        }
    }

    public String getName() {
		return name;
	}

	public StorageSite getWorkerSite() {
		return site;
	}

	protected DevXid getXid(String globalId) {
		return new DevXid(globalId, getName());
	}

	public void startTrans(String globalId) throws PEException  {
		if (site.supportsTransactions()) {
			if (StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".startTrans("+globalId+")");
                ensureConnection();
                DevXid xid = getXid(globalId);
                try {
                    dircon_getConnection().start(xid, null);
                } catch (Exception e) {
                    throw new PESQLException("Cannot start XA Transaction " + xid, e);
                }
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
                ensureConnection();
                DevXid xid = getXid(globalId);
                try {
                    dircon_getConnection().end(xid, null);
                } catch (Exception e) {
                    throw new PESQLException("Cannot end XA Transaction " + xid, e);
                }
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

                ensureConnection();
                try {
                    dircon_getConnection().prepare(getXid(globalId), promise);
                } catch (PESQLException sqlError){
                    promise.failure(sqlError);
                }
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

                ensureConnection();
                try {
                    dircon_getConnection().commit(getXid(globalId), onePhase, resultTracker);
                } catch (PESQLException e) {
                    resultTracker.failure(e);
                }
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
                ensureConnection();
                DevXid xid = getXid(globalId);
                try {
                    dircon_getConnection().rollback(xid, promise);
                } catch (Exception e) {
                    PESQLException problem = new PESQLException("Cannot rollback XA Transaction " + xid, e);
                    problem.fillInStackTrace();
                    promise.failure(problem);
                }
            }
		}

        promise.success(true);
	}

	public void close() throws PEException {
		closeWConnection();
	}

	public SingleDirectStatement getStatement() throws PESQLException {
        SingleDirectStatement workerStatement;

		try {
            ensureConnection();
            if (wSingleStatement==null) {
                setPreviousDatabaseWithCurrent();

                wSingleStatement = new SingleDirectStatement(this, dircon_getConnection());
            }
            workerStatement = wSingleStatement;
		} catch (PECommunicationsException ce) {
			processCommunicationFailure();
			throw ce;
		}

		return workerStatement;
	}

    protected void statementHadCommFailure() {
        if (statementFailureTriggersCommFailure)
            this.processCommunicationFailure();
    }

	protected void processCommunicationFailure() {
		closeWConnection();
//		lastException = PECommunicationsException.INSTANCE;
		try {
            Agent.dispatch(Singletons.require(HostService.class).getNotificationManagerAddress(), new SiteFailureNotification(site));
		} catch (PEException e1) {
			logger.error("Unable to send site failure notification", e1);
		}
	}

	public void setLastException(Exception lastException) {
		this.lastException = lastException;
	}

	public boolean isModified() throws PESQLException {
        ensureConnection();
        return dircon_getConnection().hasPendingUpdate();
    }

	public boolean hasActiveTransaction() throws PESQLException {
        ensureConnection();
        return dircon_getConnection().hasActiveTransaction();
    }

	public int getConnectionId() throws PESQLException {
        ensureConnection();
        return dircon_getConnection().getConnectionId();
    }

    public static class SingleDirectStatement implements WorkerStatement {
        public DBConnection dbConnection;
        public Worker worker;

        public SingleDirectStatement(Worker worker,DBConnection dbConnection) throws PESQLException {
            this.worker = worker;
            this.dbConnection = dbConnection;
        }

        @Override
        public void cancel() {
            worker.statementCancel(this);
        }

        @Override
        public void close() throws PESQLException {
            worker.statementClose(this);
        }

    }

    private void statementCancel(SingleDirectStatement stmt){
        if (stmt.dbConnection != null)
            stmt.dbConnection.cancel();
    }

    private void statementClose(SingleDirectStatement stmt){
        if (stmt.dbConnection != null)
            stmt.dbConnection = null;
    }

    public void writeAndFlush(CommandChannel dbConnection, MysqlMessage outboundMessage, final DelegatingResultsProcessor statementInterceptor) {
        //TODO: this dbConnection is one we handed out earlier.  Need to pull it back in.  Maybe have Worker delegate? -sgossard

        final DelegatingResultsProcessor workerResultInterceptor = new DelegatingResultsProcessor(statementInterceptor){
            boolean triggeredFailure = false;
            @Override
            public void failure(Exception e) {
                if (!triggeredFailure) {
                    PESQLException psqlError = PESQLException.coerce(e);

                    if (psqlError instanceof PECommunicationsException) {
                        statementHadCommFailure();
                    }

                    setLastException(psqlError);

                    super.failure(psqlError);
                    triggeredFailure = true;
                }
            }
        };

        if (outboundMessage != null) {

            dbConnection.writeAndFlush(outboundMessage, workerResultInterceptor);
        }
    }


    protected DBConnection dircon_getConnection() throws PESQLException {
        DirectConnectionCache.CachedConnection cacheEntry = datasourceInfo.get();
        while (cacheEntry == null){
            cacheEntry = DirectConnectionCache.checkoutDatasource(preferredEventLoop,userAuthentication, additionalConnInfo, site);

            if (datasourceInfo.compareAndSet(null, cacheEntry))
                break;

            DirectConnectionCache.returnDatasource(cacheEntry);
            cacheEntry = datasourceInfo.get();
        }
        return cacheEntry;
    }


    private void dircon_releaseConnection(boolean isStateValid) throws PESQLException {
        if (wSingleStatement != null) {
            wSingleStatement.close();
            wSingleStatement = null;
        }

        DirectConnectionCache.CachedConnection currentConnection = datasourceInfo.getAndSet(null);

        if (currentConnection != null){
            if (isStateValid) {
                DirectConnectionCache.returnDatasource(currentConnection);
            } else {
                DirectConnectionCache.discardDatasource(currentConnection);
            }
        }
    }


    public CommandChannel getDirectChannel() throws PESQLException {
        return this.dircon_getConnection();
    }

}
