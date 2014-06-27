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

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.worker.agent.Agent;
import io.netty.util.concurrent.Future;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.ISiteInstance;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.eventing.AbstractEvent;
import com.tesora.dve.eventing.AbstractReqRespState;
import com.tesora.dve.eventing.DispatchState;
import com.tesora.dve.eventing.EventHandler;
import com.tesora.dve.eventing.EventStateMachine;
import com.tesora.dve.eventing.Request;
import com.tesora.dve.eventing.Response;
import com.tesora.dve.eventing.StackAction;
import com.tesora.dve.eventing.State;
import com.tesora.dve.eventing.TransitionReqRespState;
import com.tesora.dve.eventing.TransitionResult;
import com.tesora.dve.eventing.events.AcknowledgeResponse;
import com.tesora.dve.eventing.events.ExceptionResponse;
import com.tesora.dve.eventing.events.WorkerRequestEvent;
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
public abstract class Worker implements GenericSQLCommand.DBNameResolver, EventHandler {
	
	public interface Factory {
		public Worker newWorker(UserAuthentication auth, StorageSite site) throws PEException;

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

	StorageSite site;
	UserAuthentication userAuthentication;
	
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

	Worker(UserAuthentication auth, StorageSite site) throws PEException {
		this.name = this.getClass().getSimpleName() + nextWorkerId.incrementAndGet();
		this.site = site;
		this.userAuthentication = auth;
	}

	public abstract WorkerConnection getConnection(StorageSite site, UserAuthentication auth);


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

	private void closeWConnection() throws PESQLException {
		if (wConnection != null) {
			try {
				resetStatement();
//				rollback(currentGlobalTransaction);
				wConnection.close(lastException == null);
			} catch (PEException e) {
				wConnection.close(false);
			} catch (SQLException e) {
				wConnection.close(false);
			}
			wConnection = null;
		}
	}

	public void resetStatement() throws SQLException {
		if (!StringUtils.isEmpty(currentGlobalTransaction))
			try {
				rollback(currentGlobalTransaction);
//				currentGlobalTransaction = null;
			} catch (Exception e) {
				throw new SQLException("Unable to reset worker connection", e);
//				logger.warn(getName()
//						+ " got exception on rollback during releaseWorkers() - exception ignored",
//						e);
			}
//		currentGlobalTransaction = null;
		if (chunkMgr != null) {
			chunkMgr.close();
			chunkMgr = null;
		}
	}

	public void setCurrentDatabase(PersistentDatabase currentDatabase) throws PEException {
		if (currentDatabase != null) {
			// the physical db name might be the same, but we could have dropped the physical db in between
			// check to make sure that the udb id has not changed
			String newDatabaseName = currentDatabase.getNameOnSite(site);
			if (!newDatabaseName.equals(currentDatabaseName) || (currentDatabaseID != null && currentDatabaseID.intValue() != currentDatabase.getId())) {
				previousDatabaseID = currentDatabaseID;
				currentDatabaseID = currentDatabase.getId();
				currentDatabaseName = newDatabaseName;
				userVisibleDatabaseName = currentDatabase.getUserVisibleName();
				getConnection().setCatalog(currentDatabaseName);
			}
		}
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
			wConnection = getConnection(site, userAuthentication);
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

	public DevXid getXid(String globalId) throws PEException {
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

	public void prepare(String globalId) throws PEException {
		if (site.supportsTransactions()) {
			if (!StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".prepare("+globalId+")");
				if (currentGlobalTransaction != globalId)
					throw new PEException("Transaction id mismatch (expected "
							+ currentGlobalTransaction + ", got " + globalId + ")");
				endTrans(globalId);
				getConnection().prepareXA(getXid(globalId));
			}
		}
	}

	public void commit(String globalId, boolean onePhase) throws PEException {
		if (site.supportsTransactions()) {
			if (!StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".commit("+globalId+")");
				if (currentGlobalTransaction != globalId)
					throw new PEException("Transaction id mismatch (expected "
							+ currentGlobalTransaction + ", got " + globalId + ")");
				if (onePhase)
					endTrans(globalId);
				getConnection().commitXA(getXid(globalId), onePhase);
				currentGlobalTransaction = null;
			}
		}
	}

	public void rollback(String globalId) throws PEException {
		if (site.supportsTransactions()) {
			if ( /* isModified() && */ !StringUtils.isEmpty(currentGlobalTransaction)) {
				if (logger.isDebugEnabled())
					logger.debug("Txn: "+getName()+".rollback("+globalId+")");
				if (currentGlobalTransaction != globalId)
					throw new PEException("Transaction id mismatch (expected "
							+ currentGlobalTransaction + ", got " + globalId + ")");
				endTrans(globalId);
				currentGlobalTransaction = null;
				getConnection().rollbackXA(getXid(globalId));
			}
		}
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
	
	@Override
	public Response request(Request in) {
		return sm.request(in);
	}

	@Override
	public void response(Response in) {
		sm.response(in);
	}

	@Override
	public boolean isAsynchronous() {
		return sm.isAsynchronous();
	}

	@Override
	public void requestCallbackOnly(Request in) {
		sm.requestCallbackOnly(in);
	}

	// one state for each of the blocking calls in worker
	
	// this executes by doing a Host.submit on the inbound request
	// and arranges to return the response to our own state machine, which
	// then sends the response back.  this is a transitional state.  once
	// we MysqlConnection working with events we can remove this in favor of
	// true state machines.
	private class ThreadedExecute extends TransitionReqRespState {

		@Override
		protected ChildRunnable buildRunnable(EventStateMachine ems, final Request reqEvent) {
			WorkerRequestEvent workerRequest = (WorkerRequestEvent) reqEvent;
			final DBResultConsumer resultConsumer = workerRequest.getConsumer();
			final WorkerRequest req = workerRequest.getWrappedRequest();
			return new ChildRunnable() {				
				@Override
				public void run() throws Throwable {
					// TODO Auto-generated method stub
					try {
						long reqStartTime = System.currentTimeMillis();
						req.executeRequest(Worker.this, resultConsumer);
						Worker.this.sendStatistics(req, System.currentTimeMillis() - reqStartTime);
					} catch (PEException e) {
						Worker.this.setLastException(e);
						// TODO: handle this
						//								if (e.hasCause(PECommunicationsException.class))
						//									markForPurge();
						//								else
						//									throw new PEException("On WorkerGroup " + wg, e);
						throw e;
					} catch (Exception e) {
						Worker.this.setLastException(e);
						throw e;
					}
					
				}
				
			};
		}
		
	}
		
	private final DispatchState IDLE_STATE = new DispatchState() {
		
		@Override
		public State getTarget(EventStateMachine esm, AbstractEvent event) {
			return new ThreadedExecute();
		}
		
	};
	
	private final EventStateMachine sm = new EventStateMachine("Worker",IDLE_STATE) {

		@Override
		public boolean isAsynchronous() {
			return false;
		}
		
	};
	
}
