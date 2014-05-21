package com.tesora.dve.server.connectionmanager;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.tesora.dve.db.mysql.common.MysqlHandshake;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSet;
import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.common.RemoteException;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.logutil.DisabledExecutionLogger;
import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.comms.client.messages.BeginTransactionRequest;
import com.tesora.dve.comms.client.messages.CommitTransactionRequest;
import com.tesora.dve.comms.client.messages.ConnectRequest;
import com.tesora.dve.comms.client.messages.DBMetadataRequest;
import com.tesora.dve.comms.client.messages.FetchRequest;
import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.GetDatabaseRequest;
import com.tesora.dve.comms.client.messages.GetTransactionStatusRequest;
import com.tesora.dve.comms.client.messages.GlobalRecoveryRequest;
import com.tesora.dve.comms.client.messages.PreConnectRequest;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.comms.client.messages.RollbackTransactionRequest;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.dbc.AddConnectParametersRequest;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.groupmanager.PurgeWorkerGroupCaches;
import com.tesora.dve.groupmanager.SiteFailureMessage;
import com.tesora.dve.infomessage.ConnectionMessageManager;
import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.lockmanager.AcquiredLockSet;
import com.tesora.dve.lockmanager.LockClient;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.server.connectionmanager.messages.AddConnectParametersExecutor;
import com.tesora.dve.server.connectionmanager.messages.AgentExecutor;
import com.tesora.dve.server.connectionmanager.messages.BeginTransactionRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.CommitTransactionRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.ConnectRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.DBMetadataRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.FetchRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.GetDatabaseRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.GetTransactionStatusRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.GlobalRecoveryExecutor;
import com.tesora.dve.server.connectionmanager.messages.PreConnectRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.PurgeWorkerGoupCacheExecutor;
import com.tesora.dve.server.connectionmanager.messages.RollbackTransactionRequestExecutor;
import com.tesora.dve.server.connectionmanager.messages.SiteFailureExecutor;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerCommitRequest;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerPrepareRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.server.messaging.WorkerRollbackRequest;
import com.tesora.dve.server.transactionmanager.Transaction2PCTracker;
import com.tesora.dve.sql.schema.ConnectionContext;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.schema.cache.NonMTCachedPlan;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.variable.SessionVariableHandler;
import com.tesora.dve.variable.VariableInfo;
import com.tesora.dve.variable.VariableValueStore;
import com.tesora.dve.worker.agent.Agent;
import com.tesora.dve.worker.StatementManager;
import com.tesora.dve.worker.UserAuthentication;
import com.tesora.dve.worker.UserCredentials;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class SSConnection extends Agent implements WorkerGroup.Manager, LockClient {

	public static final String DYNAMIC_POLICY_VARIABLE_NAME = "dynamic_policy";
	public static final String STORAGE_GROUP_VARIABLE_NAME = "storage_group";

	static Logger logger = Logger.getLogger(SSConnection.class);

	static final boolean transactionLoggingEnabled = !Boolean.getBoolean("SSConnection.suppressTransactionRecording");
	
	static ConnectionSemaphore connectionCount = new ConnectionSemaphore(100);

	static Map<String, AgentExecutor<SSConnection>> executorMap = new HashMap<String, AgentExecutor<SSConnection>>() {
		private static final long serialVersionUID = 1L;
		{
			put(PreConnectRequest.class.getName(), new PreConnectRequestExecutor());
			put(ConnectRequest.class.getName(), new ConnectRequestExecutor());
			put(AddConnectParametersRequest.class.getName(), new AddConnectParametersExecutor());
			put(FetchRequest.class.getName(), new FetchRequestExecutor());
			put(DBMetadataRequest.class.getName(), new DBMetadataRequestExecutor());
			put(GetTransactionStatusRequest.class.getName(), new GetTransactionStatusRequestExecutor());
			put(BeginTransactionRequest.class.getName(), new BeginTransactionRequestExecutor());
			put(CommitTransactionRequest.class.getName(), new CommitTransactionRequestExecutor());
			put(RollbackTransactionRequest.class.getName(), new RollbackTransactionRequestExecutor());
			put(GetDatabaseRequest.class.getName(), new GetDatabaseRequestExecutor());
			put(SiteFailureMessage.class.getName(), new SiteFailureExecutor());
			put(PurgeWorkerGroupCaches.class.getName(), PurgeWorkerGoupCacheExecutor.INSTANCE);
			put(GlobalRecoveryRequest.class.getName(), new GlobalRecoveryExecutor());
		}
	};

	private final AtomicInteger requestCounter = new AtomicInteger();

	Map<StorageGroup, WorkerGroup> availableWG = new HashMap<StorageGroup, WorkerGroup>();
	Map<StorageGroup, WorkerGroup> activeWG = new HashMap<StorageGroup, WorkerGroup>();

	MysqlHandshake connHandshake;

	DBNative dbNative;
	NativeCharSet clientCharSet = MysqlNativeCharSet.UTF8;
	
	public NativeCharSet getClientCharSet() {
		return clientCharSet;
	}

	public void setClientCharSet(NativeCharSet clientCharSet) {
		this.clientCharSet = clientCharSet;
	}

	SchemaEdge<Database<?>> currentDatabase;
	SchemaEdge<PEUser> user;
	SchemaEdge<IPETenant> currentTenant;

	SchemaContext schemaContext;
	SlowQueryLogger slowQueryLogger = null;
	// show warnings, errors support
	ConnectionMessageManager messages = new ConnectionMessageManager();
	
	CatalogDAO txnCatalogDAO = null;
	
	int currentConnId;

	int transactionDepth = 0;
	String currentTransId = null;
	boolean autoCommitMode = true;
	
	long lastInsertedId = 0;
	
	VariableValueStore sessionVariables = new VariableValueStore("Session");
	VariableValueStore userVariables = new VariableValueStore("User",false);

	AcquiredLockSet txnLocks = new AcquiredLockSet();
	AcquiredLockSet nontxnLocks = new AcquiredLockSet();
	
	ClusterLock addGenerationReadLock = null; // set during txns to lock out addGeneration
	
	Map<String, SSStatement> statementMap = new HashMap<String, SSStatement>();
	
	// pertains to prepared stmts
	private Map<String, MyPreparedStatement<String>> pStmtMap = new HashMap<String, MyPreparedStatement<String>>();

	
	private ReplicationOptions replOpt = null;

	private boolean tempCleanupRequired = false;

	private int uniqueValue = 0;
	private long lastMessageProcessedTime = System.currentTimeMillis();
	private String cacheName = NonMTCachedPlan.GLOBAL_CACHE_NAME;
	private String sessionVariableSetStatement;
	private boolean executingInContext;
	private ClientCapabilities clientCapabilities = new ClientCapabilities();
	
	public SSConnection() throws PEException {
		super("SSConnection");
	}
	
	@Override
	protected void initialize() throws PEException {
		connectionCount.nonBlockingAcquire();
        currentConnId = GroupManager.getCoordinationServices().registerConnection(Singletons.require(HostService.class).getHostName() + "." + getName());
		PerHostConnectionManager.INSTANCE.registerConnection(currentConnId, this);
        dbNative = Singletons.require(HostService.class).getDBNative();
		connHandshake = new SSConnectionHandshake(currentConnId);
		schemaContext = SchemaContext.createContext(this);
		
		super.initialize();
	}
	
	protected SSConnection(String name) {
		this.name = name;
		schemaContext = SchemaContext.createContext(this);
	}

	// close() is synchronized as it can be called during message processing if
	// the client connection closes
	// TODO: the netty handler should send a message to the SSConnection to close, rather
	// than calling close() directly, then there would be no need to synchronize.
	@Override
	public synchronized void close() throws PEException {
		releaseGenerationLock();
		if (activeWG != null) {
			StatementManager.INSTANCE.cancelAllStatements(currentConnId);
			PerHostConnectionManager.INSTANCE.unRegisterConnection(currentConnId);
			GroupManager.getCoordinationServices().unRegisterConnection(currentConnId);
			try {
				if (transactionDepth > 0) 
					userRollbackTransaction();
			} catch (Exception e) {
			}
			executeCleanupSteps();
			clearAllWorkerGroups();
			super.close();
			connectionCount.release();
			activeWG = null;
			txnLocks.release();
			nontxnLocks.release();
		}
	}

	private void initialiseSessionVariables() throws PEException {
        sessionVariables.setAll(Singletons.require(HostService.class).getSessionConfigDefaults(this));
	}

	// onMessage() and close() are synchronized as close() can be called
	// during message processing if the client connection is closed.
	@Override
	public synchronized void onMessage(final Envelope e) throws PEException {
		ResponseMessage resp;
		try {
			String executorClassName = e.getPayload().getClass().getName();
			final AgentExecutor<SSConnection> ssConnExec = executorMap.get(executorClassName);
			final SSConnection thisSSCon = this;
			if (ssConnExec != null) {
				resp = executeInContext(new Callable<ResponseMessage>() {
					public ResponseMessage call() throws Exception {
						try {
							return ssConnExec.execute(thisSSCon, e.getPayload());
						} catch (Exception e) {
							throw e;
						} catch (Throwable e) {
							throw new PEException("SSConnection.onMessage call generated exception", e);
						}
					}
				});
			} else {
				throw new PECodingException("Unable to find executor for " + executorClassName);
			}
		} catch (PEException re) {
			resp = new GenericResponse().setException(re);
		} catch (Throwable t) {
			resp = new GenericResponse().setException(new RemoteException(getName(), t));
		}

		if ( resp != null )
			returnResponse(e, resp);
	}

	public <T> T executeInContext(Callable<T> task) throws Exception {
		T returnValue;

		synchronized (this) {
			pushDebugContext(nextRequestId());
			try {
				acquireGenerationLock();
				try {
					executingInContext = true;
					returnValue = task.call();
				} finally {
					if (transactionDepth == 0) {
						releaseGenerationLock();
					}
					purgeDynamicWorkerGroups();

					//						setCatalogDAO(null).close();
					nontxnLocks.release();
					lastMessageProcessedTime = System.currentTimeMillis();
					executingInContext = false;
				}

				doPostReplyProcessing();
			} finally {
				PEThreadContext.popFrame();
			}
		}
		return returnValue;
	}

	private String nextRequestId() {
		final int id;
		if (requestCounter.compareAndSet(Integer.MAX_VALUE, 1))
			id = 1;
		else
			id = requestCounter.incrementAndGet();
		return Integer.toString(id);
	}

	private void pushDebugContext(final String requestId) {
		PEThreadContext.pushFrame(this.toString())
				.put("connectionId", Integer.toString(currentConnId))
				.put("requestId", requestId);
		if (transactionDepth > 0)
			PEThreadContext.put("txnDepth", Integer.toString(transactionDepth));
		if (currentTransId != null)
			PEThreadContext.put("txn", currentTransId);
		PEThreadContext.logDebug();
	}

	private void acquireGenerationLock() {
		try{ // TOPERF
            if (addGenerationReadLock == null) {
                addGenerationReadLock = GroupManager.getCoordinationServices().getClusterLock(RangeDistributionModel.GENERATION_LOCKNAME);
                addGenerationReadLock.sharedLock(this,"default statement generation lock");
            }
        } catch (RuntimeException t){
            logger.warn("Problem getting generation lock",t);
            throw t;
        }
    }

	private void releaseGenerationLock() {
        if (addGenerationReadLock != null) {
			addGenerationReadLock.sharedUnlock(this,"releasing generation lock");
			addGenerationReadLock = null;
		}
	}

	public ResponseMessage executeMessageDirect(final Object message) throws Exception {
		final SSConnection contextSSCon = this;
		return executeInContext(new Callable<ResponseMessage>() {
			public ResponseMessage call() throws Exception {
				ResponseMessage resp;

				if (executorMap.containsKey(message.getClass().getName())) {
					try {
						AgentExecutor<SSConnection> ssConnExec = executorMap.get(message.getClass().getName());
						resp = ssConnExec.execute(contextSSCon, message);
					} catch (PEException re) {
						resp = new GenericResponse().setException(re);
					} catch (Throwable t) {
						resp = new GenericResponse().setException(new RemoteException(getName(), t));
					}
				} else {
					resp = new GenericResponse().setException(new PEException(
							"No SSConnectionExecutor defined for message of type "
									+ message.toString()));
				}		
				return resp;
			}
		});
	}
	
	@Override
	public synchronized void onTimeout() {
		
		doSessionTimeoutCheck();
	}

	private void doSessionTimeoutCheck() {
		try {
			final long wait_timeout = Long.parseLong(getSessionVariable(SessionVariableHandler.WAIT_TIMEOUT));
			if (!executingInContext) {
				Singletons.require(HostService.class).submit(new Callable<Void>() {
                    public Void call() throws Exception {
                        try {
                            long secondsSinceLastMessage = (System.currentTimeMillis() - lastMessageProcessedTime) / 1000;

                            if (wait_timeout > 0 && secondsSinceLastMessage > wait_timeout) {
                                if (logger.isDebugEnabled())
                                    logger.debug("NPE: SSConnection{" + getName() + "}.onTimeout calls close() due to wait_timeout exceeded");
                                close();
                            } else if (currentTransId == null && secondsSinceLastMessage > 10) {
                                try {
                                    synchronized (SSConnection.this) {
//										if (logger.isDebugEnabled())
//											logger.debug("NPE: SSConnection{"+getName()+"}.onTimeout calls clearAllWorkerGroups() due to non-transactional timeout");
                                        clearAllWorkerGroups();
                                    }
                                } catch (PEException e) {
                                    logger.warn("Exception encountered clearing worker groups from connection", e);
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("Exception executing timeout check", e);
                        }
                        return null;
                    }
                });
			}
		} catch (PENotFoundException e) {
			// Ignore - the wait_timeout variable won't be found if it takes more than the timeout interval
			// for the ConnectRequest packet to be received
		} catch (Exception e) {
			logger.warn("Exception executing timeout check", e);
		}
	}

	public void doPostReplyProcessing() throws PEException {
		if ( tempCleanupRequired && transactionDepth == 0) {
			try {
				executeCleanupSteps();
			} catch (PEException e) {
				logger.warn("Exception during SSConnection post reply processing (temp cleanup)", e);
			}
			tempCleanupRequired = false;
		}
		clearCatalogDAO();
	}

	private void purgeDynamicWorkerGroups() throws PEException {
		// After each statement, we clear out any dynamic groups that we have in
		// the workergroup cache
		for (Iterator<WorkerGroup> iwg = availableWG.values().iterator(); iwg.hasNext();) {
			WorkerGroup wg = iwg.next();
			if (wg.isTemporaryGroup()) {
				iwg.remove();
				WorkerGroupFactory.returnInstance(this, wg);
			}
		}
	}
	
	public void setTempCleanupRequired(boolean required) {
		tempCleanupRequired = required;
	}

	void doBeginTransaction() {
		currentTransId = UUID.randomUUID().toString();
		setTempCleanupRequired(false);
	}
	
	void doFinalCommit(WorkerRequest req) throws PEException {
		try {
			doTerminateTransaction(req);
		} catch (PEException t) {
			try {
				doRollbackTransaction();
			} catch (Throwable t2) {
				logger.error("Exception during transaction rollback", t2);
				t2.printStackTrace();
			}
			throw t;
		}
	}
	
	void doCommitOnePhase() throws PEException {
        try{
		    doFinalCommit(new WorkerCommitRequest(getTransactionalContext(), true));
        } catch (Throwable t) {
            try {
                doRollbackTransaction();
            } catch (Throwable t2) {
                logger.error("Exception encountered during rollback", t2);
            }
            throw new PEException("Exception encountered during Two Phase Commit", t);
        }
	}
	
	void doCommitTwoPhase() throws PEException {

		// Build the txn record to use for rollforward
		Transaction2PCTracker txnTracker = null;
		if (transactionLoggingEnabled) {
			txnTracker = new Transaction2PCTracker(currentTransId);
			txnTracker.startPrepare();
		}

		try {
			
			// Prepare the transaction
			WorkerRequest req = new WorkerPrepareRequest(getTransactionalContext());
			WorkerGroup.executeOnAllGroups(availableWG.values(), MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE);
			
//			int msgCount = 0;
//			for (WorkerGroup wg : availableWG.values()) {
//				wg.sendToAllWorkers(this, req);
//				msgCount += wg.size();
//			}
//			consumeReplies(msgCount);
			
			// Commit the transaction record
			if (transactionLoggingEnabled) {
				txnTracker.startCommit();
			}
			
			// Commit the transaction itself
			doFinalCommit(new WorkerCommitRequest(getTransactionalContext(), false));

		} catch (Throwable t) {
			try {
				doRollbackTransaction();
			} catch (Throwable t2) {
				logger.error("Exception encountered during rollback", t2);
			}
			throw new PEException("Exception encountered during Two Phase Commit", t);
		} finally {
			// Remove the transaction record
			if (transactionLoggingEnabled) {
				txnTracker.clearTransactionRecord();
			}
		}
	}

	void doCommitTransaction() throws PEException {
		availableWG.putAll(activeWG);
		activeWG.clear();
		if (currentTransId != null) {
			int modifiedWorkerCount = 0;
			for (WorkerGroup wg : availableWG.values())
				modifiedWorkerCount += wg.activeTransactionCount();
			
			if (modifiedWorkerCount > 1)
				doCommitTwoPhase();
			else 
				doCommitOnePhase();
		}
		setTempCleanupRequired(true);
		transactionCleanup();
	}

	void doRollbackTransaction() throws PEException {
		doTerminateTransaction(new WorkerRollbackRequest(getTransactionalContext()));
		transactionCleanup();
	}
	
	void doTerminateTransaction(WorkerRequest req) throws PEException, PEException {
		List<Future<Worker>> committedWorkers = new ArrayList<Future<Worker>>();

        boolean isOnePhase = false;
        if (req instanceof WorkerCommitRequest){
            isOnePhase = ((WorkerCommitRequest)req).isOnePhase();
        }

		for (WorkerGroup wg : availableWG.values())
			committedWorkers.addAll(
					wg.submit(MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE));
		
		for (Future<Worker> f : committedWorkers) {
			// At this point we have recorded the transaction as committed.  If any of the sites fail
			// to commit, we push on and commit the rest anyways, so just log exceptions and keep 
			// processing the replies from the other sites.  If the site is brought back online and 
			// made the master again, the "set master" functionality will recover the transaction.
			try {
				f.get();
			} catch (Throwable t) {
                if (isOnePhase) {
                    Throwable bestCause = (t instanceof ExecutionException ? t.getCause() : t); //unwrap if needed, so we can traverse pe exception chain.
                    throw new PEException(t.getMessage(),bestCause);
                } else
				    logger.warn("Site failed during XA commit phase - exception ignored", t);
			}
		}
		
//		WorkerGroup.executeOnAllGroups(availableWG.values(), MappingSolution.AllWorkers, req, resultConsumer);
//		int receiveCount = 0;
//		for (WorkerGroup wg : availableWG.values()) {
//			wg.sendToAllWorkers(this, req);
//			receiveCount += wg.size();
//		}
//		while (receiveCount-- > 0) {
//			// At this point we have recorded the transaction as committed.  If any of the sites fail
//			// to commit, we push on and commit the rest anyways, so just log exceptions and keep 
//			// processing the replies from the other sites.  If the site is brought back online and 
//			// made the master again, the "set master" functionality will recover the transaction.
//			try {
//				receive();
//			} catch (Throwable t) {
//				logger.warn("Site failed during XA commit phase - exception ignored", t);
//			}
//		}

		setTempCleanupRequired(true);
	}

	public void autoBeginTransaction() {
		if (currentTransId == null)
			doBeginTransaction();
	}

	public void autoCommitTransaction() throws Throwable {
		if (autoCommitMode && transactionDepth == 0) {
			doCommitTransaction();
		}
	}

	public void autoRollbackTransaction() throws PEException, PEException {
        this.autoRollbackTransaction(false);
    }

    public void autoRollbackTransaction(boolean force) throws PEException, PEException {
		if (force || transactionDepth == 0) {
			availableWG.putAll(activeWG);
			activeWG.clear();
			doRollbackTransaction();
		}
	}
	
	public void implicitCommit() throws PEException {
		if (currentTransId != null && availableWG.size() > 0)
			doCommitTransaction();
		transactionCleanup();
	}
	
	void transactionCleanup() throws PEException {
		transactionDepth = 0;
		currentTransId = null;
		activeWG.clear();
//		clearWorkerGroups();
		setTempCleanupRequired(true);
		txnLocks.release();
	}
	
	public void userBeginTransaction() throws PEException {
		userBeginTransaction(false);
	}

	public void userBeginTransaction(boolean withConsistentSnapshot) throws PEException {
		if (transactionDepth++ == 0)
			doBeginTransaction();
		
		if (withConsistentSnapshot) {

            ClusterLock genLock = GroupManager.getCoordinationServices().getClusterLock(RangeDistributionModel.GENERATION_LOCKNAME);
			genLock.exclusiveLock(this,"start transaction with consistent snapshot");
			try {
				// Execute select against one table in each PersistentGroup to create snapshot
				final CatalogDAO catalogDAO = getCatalogDAO();
				List<PersistentGroup> allGroups = catalogDAO.findAllPersistentGroups();
				List<UserTable> allTables = catalogDAO.findAllUserTables();
				Map<PersistentGroup, UserTable> tablesByGroupMap = new HashMap<PersistentGroup, UserTable>();
				for (UserTable table : allTables) {
					if (table.isSynthetic())
						continue;
					tablesByGroupMap.put(table.getPersistentGroup(), table);
					if (tablesByGroupMap.size() == allGroups.size())
						break;
				}
				for (Map.Entry<PersistentGroup, UserTable> entry : tablesByGroupMap.entrySet()) {
					final UserTable ut = entry.getValue();
					SQLCommand sqlCommand = new SQLCommand("select * from " + dbNative.getNameForQuery(ut)  + " where 0=1");
					WorkerGroup wg = getWorkerGroupAndPushContext(entry.getKey(),null);
					try {
						WorkerExecuteRequest req = 
								new WorkerExecuteRequest(getTransactionalContext(), sqlCommand);
						req.onDatabase(ut.getDatabase());
						wg.execute(MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE);
//						wg.sendToAllWorkers(this, req);
//						consumeReplies(wg.size());
					} finally {
						returnWorkerGroupAndPopContext(wg);
					}
				}
			} finally {
				genLock.exclusiveUnlock(this,"releasing exclusive generation lock");
			}
		}
	}
	
	public void userCommitTransaction() throws Throwable {
		if (transactionDepth > 0) {
			--transactionDepth;
			if (activeWG.size() > 1)  // we're executing on an active txn as QueryPlan doesn't know we're commit
				throw new PEException("Commit called while operation is active");
			for (SSStatement stmt : statementMap.values()) {
				stmt.clearQueryPlan();
			}
//			availableWG.putAll(activeWG);
			doCommitTransaction();
		}
	}

	public void userRollbackTransaction() throws PEException {
		if (transactionDepth > 0) {
			availableWG.putAll(activeWG);
			activeWG.clear();
			doRollbackTransaction();
		} 
//		else
//			throw new PEException("rollback called when no transaction is in progress");
	}
	
	public void setAutoCommitMode(boolean isAutoCommit) throws PEException {
		if (!autoCommitMode && isAutoCommit == true)
			try {
				userCommitTransaction();
			} catch (Throwable e) {
				throw new PEException("Unable to set autocommit mode", e);
			}
		autoCommitMode = isAutoCommit;
	}
	
	public CatalogDAO getCatalogDAO() {
		if (txnCatalogDAO == null)
			txnCatalogDAO = CatalogDAOFactory.newInstance();
		return txnCatalogDAO;
	}
	
	public void clearCatalogDAO() {
		if (txnCatalogDAO != null) {
			txnCatalogDAO.close();
			txnCatalogDAO = null;
		}
	}
	
	@SuppressWarnings("unchecked")
	public void startConnection(UserCredentials userCred) throws PEException {
		User persistentUser = userCred.authenticate(this);
		PerHostConnectionManager.INSTANCE.setUserInfo(getConnectionId(), persistentUser.getName());
		initialiseSessionVariables();
		user = StructuralUtils.buildEdge(schemaContext, schemaContext.getSource().find(schemaContext, PEUser.getUserKey(persistentUser)), true);
	}
	
	public SchemaEdge<PEUser> getUser() {
		return user;
	}
	
	public SchemaEdge<Database<?>> getCurrentDatabaseEdge() {
		return currentDatabase;
	}

	public Database<?> getCurrentDatabase() {
		if (currentDatabase == null) return null;
		return currentDatabase.get(schemaContext);
	}
	
	@SuppressWarnings("unchecked")
	public void setCurrentDatabase(Database<?> db) {
		currentDatabase = StructuralUtils.buildEdge(schemaContext, db, true);
		schemaContext.clearPolicyContext();
	}
	
	public UserDatabase getPersistentDatabase() {
		if (currentDatabase == null) return null;
		Database<?> db = currentDatabase.get(schemaContext);
		if (db == null) return null;
		schemaContext.beginSaveContext();
		try {
			return db.getPersistent(schemaContext);
		} finally {
			schemaContext.endSaveContext();
		}
	}
	
	public void setPersistentDatabase(UserDatabase givenDatabase) throws PEException {
		PerHostConnectionManager.INSTANCE.setConnectionDB(currentConnId, givenDatabase.getName());
		setCurrentDatabase(schemaContext.findDatabase(givenDatabase.getName()));
	}
	
	public void setCurrentDatabase(String databaseName) throws PEException {
		setPersistentDatabase(getCatalogDAO().findDatabase(databaseName));
	}

	public IPETenant getCurrentTenant() {
		if (currentTenant == null) return null;
		return currentTenant.get(schemaContext);
	}
	
	public String getCurrentTenantId() {
		// accessed through reflection
		String tenantId = null;
		IPETenant ten = getCurrentTenant();
		if (ten != null)
			tenantId = Long.toString(ten.getTenantID());
		return tenantId; 
	}
	
	@SuppressWarnings("unchecked")
	public void setCurrentTenant(IPETenant tenant) {
		currentTenant = StructuralUtils.buildEdge(schemaContext, tenant, true);
		schemaContext.clearPolicyContext();
	}
		
	public DBNative getDBNative() {
		return dbNative;
	}

	public WorkerGroup getWorkerGroup(StorageGroup sg, PersistentDatabase ctxDB) throws PEException {
		WorkerGroup wg = null;
		if (activeWG == null)
			throw new PEException(getName() + " has already been closed - cannot get WorkerGroup");
		if (activeWG.containsKey(sg))
			throw new PEException("Cannot re-allocate active StorageGroup " + sg);
		if (availableWG.containsKey(sg)) {
			wg = availableWG.remove(sg);
			if (ctxDB != null)
				wg.setDatabase(this,ctxDB);
		} else {
			wg = WorkerGroupFactory.newInstance(this, sg, ctxDB);
		}
		wg.setManager(this);
		wg.associateWithConnection(currentConnId);
		activeWG.put(sg, wg);
		for (WorkerGroup awg : availableWG.values())
			if (awg == wg)
				throw new PECodingException("WorkerGroup" + wg.getDisplayId() + " is still in "+getName()+" available list at allocation");

		return wg;
	}

	
	@Override
	public void returnWorkerGroup(WorkerGroup wg) throws PEException {
		StorageGroup sg = wg.getGroup();
		wg.disassociateFromConnection();
		if (activeWG.containsKey(sg) && wg == activeWG.get(sg)) {
			activeWG.remove(sg);
			if (wg.isMarkedForPurge())
				WorkerGroupFactory.purgeInstance(this, wg);
			else
				availableWG.put(sg, wg);
		} else {
			// This snippet addresses an issue where worker groups are given 
			// back to the wg factory, even though they are still in the available list
			boolean stillInAvailableList = false;
			for (Iterator<Entry<StorageGroup, WorkerGroup>> iWg = availableWG.entrySet().iterator(); iWg.hasNext();)
				if (iWg.next().getValue() == wg) {
					stillInAvailableList = true;
					break;
				}
			if (!stillInAvailableList)
				WorkerGroupFactory.returnInstance(this, wg);
		}
	}

	public WorkerGroup getWorkerGroupAndPushContext(StorageGroup sg, PersistentDatabase contextDB) throws PEException {
		WorkerGroup wg = getWorkerGroup(sg, contextDB);
		wg.pushDebugContext();
		return wg;
	}

	public void returnWorkerGroupAndPopContext(WorkerGroup wg) throws PEException {
		try {
			returnWorkerGroup(wg);
		} finally {
			PEThreadContext.popFrame();
		}
	}

	
	public void swapWorkerGroup(WorkerGroup oldWG, WorkerGroup newWG) {
		StorageGroup oldSG = oldWG.getGroup();
		if (activeWG.containsKey(oldSG) && oldWG == activeWG.get(oldSG)) {
			oldWG.disassociateFromConnection();
			activeWG.remove(oldSG);
		}
		oldWG.markForPurge();
		activeWG.put(newWG.getGroup(), newWG);
	}
	
	private void clearTemporaryWorkerGroups() throws PEException {
		if (activeWG.size() > 0)
			throw new PEException("transaction terminated with active workers");
//		for (Iterator<Map.Entry<StorageGroup, WorkerGroup>> i = availableWG.entrySet().iterator(); i.hasNext();) {
//			StorageGroup sg = i.next().getKey();
//			if (sg.isTemporaryGroup()) {
//				WorkerGroupFactory.returnInstance(this, availableWG.get(sg));
//				i.remove();
//			}
//		}
		clearAllWorkerGroups();
	}

	public void clearAllWorkerGroups() throws PEException {
//		try {
//			for (WorkerGroup wg : availableWG.values()) {
//				if (logger.isDebugEnabled())
//					logger.debug("NPE: SSConnection{"+getName()+"}.clearAllWorkerGroups() clears "+wg);
//				WorkerGroupFactory.returnInstance(this, wg);
//			}
//		} finally {
//			availableWG.clear();
//		}
		for (Iterator<WorkerGroup> iwg = availableWG.values().iterator(); iwg.hasNext();) {
			WorkerGroup wg = iwg.next();
			if (logger.isDebugEnabled())
				logger.debug("NPE: SSConnection{"+getName()+"}.clearAllWorkerGroups() clears "+wg);
			iwg.remove();
			WorkerGroupFactory.returnInstance(this, wg);
		}
	}
	
	public MysqlHandshake getHandshake() {
		return connHandshake;
	}

	public int getConnectionId() {
		return currentConnId;
	}

	public String getTransId() {
		return currentTransId;
	}

	public long getLastInsertedId() {
		return lastInsertedId;
	}

	public void setLastInsertedId(long lastInsertedId) {
		this.lastInsertedId = lastInsertedId;
	}

	public boolean hasActiveTransaction() {
		return transactionDepth > 0;
	}
	
	public boolean hasActiveXATransaction() {
		return hasActiveTransaction();
	}
	
	public SchemaContext getSchemaContext() {
		return schemaContext;
	}
	
	private void executeCleanupSteps() throws PEException {
		if (activeWG != null) {
			availableWG.putAll(activeWG);
			activeWG.clear();
			clearTemporaryWorkerGroups();
		}
	}

	public void clearStatementResources(SSStatement stmt) throws PEException {
		setTempCleanupRequired(true);
		stmt.clearQueryPlan();
	}

	public Map<String, String> getSessionVariables() throws PEException {
		HashMap<String, String> vMap = new HashMap<String, String>();
		for (String name : sessionVariables.getNames())
			vMap.put(name, getSessionVariable(name));
		return Collections.unmodifiableMap(vMap);
	}
	
	public void setSessionVariable(String variableName, String value) throws PEException {
		synchronized (sessionVariables) {
            SessionVariableHandler handler = Singletons.require(HostService.class).getSessionConfigTemplate().getVariableInfo(variableName).getHandler();
			handler.setValue(this, variableName, value);
			handler.onValueChange(variableName, value);
		}
	}
	
	public String getSessionVariable(String variableName) throws PEException {
        return Singletons.require(HostService.class).getSessionConfigTemplate().getVariableInfo(variableName).getHandler().getValue(this, variableName);
	}

	public boolean hasSessionVariable(String variableName) throws PEException {
        return Singletons.require(HostService.class).getSessionConfigTemplate().getVariableInfo(variableName, false) != null;
	}
	
	public void setUserVariable(String variableName, String value) throws PENotFoundException {
		userVariables.setValue(variableName, value);
	}
	
	public String getUserVariable(String variableName) throws PENotFoundException {
		return userVariables.getValue(variableName);
	}

	public Map<String, String> getUserVariables() {
		return userVariables.getVariableMap();
	}
	
	public void updateWorkerState(String assignmentClause) throws PEException {
		sendToAllGroups(new WorkerSetSessionVariableRequest(getNonTransactionalContext(), assignmentClause));
	}
	
	void sendToAllGroups(WorkerRequest req) throws PEException {
		List<WorkerGroup> allGroups = new ArrayList<WorkerGroup>(availableWG.values());
		allGroups.addAll(activeWG.values());
		
		WorkerGroup.executeOnAllGroups(allGroups, MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE);
		
//		int receiveCount = 0;
//		
//		for (WorkerGroup wg : allGroups) {
//			wg.sendToAllWorkers(this, req);
//			receiveCount += wg.size();
//		}
//		consumeReplies(receiveCount);
	}

	public ResultCollector getSessionVariableAsResult(String variableName) throws PEException {
        return Singletons.require(HostService.class).getSessionConfigTemplate().getVariableInfo(variableName).getHandler().getValueAsResult(this, variableName);
	}
	
	public PersistentGroup getCurrentPersistentGroup() throws PEException {
		return getCatalogDAO().findPersistentGroup(STORAGE_GROUP_VARIABLE_NAME);
	}
	
	public DynamicPolicy getCurrentDynamicPolicy() throws PEException {
		return getCatalogDAO().findDynamicPolicy(DYNAMIC_POLICY_VARIABLE_NAME);
	}

	public UserAuthentication getUserAuthentication() {
		return user.get(schemaContext).getUserAuthentication();
	}

	public void onTransactionAbortedByJDBC() throws PEException {
		transactionCleanup();
	}
	
	public void acquireLock(LockSpecification ls, LockType type) {
		if (hasActiveTransaction() && ls.isTransactional()) {
			txnLocks.acquire(this, ls, type);
		} else {
			nontxnLocks.acquire(this, ls, type);
		}
	}
	
	// used by ServerDBConnection - toss any non txn locks
	public void releaseNonTxnLocks() {
		nontxnLocks.release();
	}
	
	public boolean isAutoCommitMode() {
		return autoCommitMode;
	}

	public long getUniqueValue() {
		return ++uniqueValue ;
	}

	public void setSessionVariableValue(String name, String value) throws PENotFoundException {
		synchronized (sessionVariables) {
			sessionVariables.setValue(name, value);
			sessionVariableSetStatement = null;
		}
	}

	public String getSessionVariableValue(String name) throws PENotFoundException {
		return sessionVariables.getValue(name);
	}

	public SSContext getTransactionalContext() {
		return new SSContext(currentConnId, currentTransId);
	}
	
	public SSContext getNonTransactionalContext() {
		return new SSContext(currentConnId);
	}

	public boolean isUsingSiteInstance(int siteInstanceId) {
		return false;
	}
	
	/**
	 * Any WorkerGroups cached in, but not used by, this connection are released.  If this connection has any 
	 * WorkerGroups in the active state, this connection is in the middle of a transaction so the connection is
	 * closed.
	 * 
	 * @param siteInstanceId
	 * @throws PEException
	 */
	public void purgeSiteInstance(int siteInstanceId) throws PEException {
		for (Iterator<WorkerGroup> iwg = availableWG.values().iterator(); iwg.hasNext();) {
			WorkerGroup wg = iwg.next();
			if (wg.usesSiteInstance(this, siteInstanceId)) {
				iwg.remove();
				logger.warn("WorkerGroup " + wg + " removed from connection " + getName() + " due to failure of "
						+ "site with id " + siteInstanceId);
			} 
		}

		for (WorkerGroup wg : activeWG.values()) {
			if (wg.usesSiteInstance(this, siteInstanceId)) {
				close();
				logger.warn("Connection " + getName() + " closed due to failure of "
						+ "site with id " + siteInstanceId);
				break;
			}
		}
	}
	

	public ConnectionContext getConnectionContext() {
		return schemaContext.getConnection();
	}

	public void clearWorkerGroupCache(StorageGroup group) {
		for(Iterator<WorkerGroup> iwg = availableWG.values().iterator(); iwg.hasNext();) {
			if (iwg.next().getGroup().equals(group))
				iwg.remove();
		}
	}
	
	public String getCacheName() {
		return this.cacheName;
	}

	public void setCacheName(String cacheName) {
		this.cacheName  = cacheName;
	}
	
	public ReplicationOptions getReplicationOptions() {
		if (replOpt == null) {
			replOpt = new ReplicationOptions();
		}
		return replOpt;
	}
	
	public ExecutionLogger getExecutionLogger() {
		if (slowQueryLogger == null)
			return DisabledExecutionLogger.defaultDisabled;
		return slowQueryLogger;
	}
	
	public ExecutionLogger getNewPlanLogger(QueryPlan qp) {
		if (SchemaVariables.slowQueryLogEnabled(getConnectionContext())) {
			slowQueryLogger = new SlowQueryLogger(this,qp);
			return slowQueryLogger;
		}
		return DisabledExecutionLogger.defaultDisabled;
	}
	
	public void clearSlowQueryLogger() {
		slowQueryLogger = null;
	}

	public ConnectionMessageManager getMessageManager() {
		return messages;
	}
	
	public static void setMaxConnections(int newMax) {
		connectionCount.adjustMaxConnections(newMax);
	}

	public static void setMinMemoryRequirement(int newMin) {
		ConnectionSemaphore.adjustMinMemory(newMin);
	}
	
	@Override
	public String toString() {
		return getName();
	}

	public String getSessionVariableSetStatement() throws PEException {
		synchronized (sessionVariables) {
			if (sessionVariableSetStatement == null) {
				sessionVariableSetStatement = "SET ";
				int clause = 0;
                for (VariableInfo<SessionVariableHandler> vInfo : Singletons.require(HostService.class).getSessionConfigTemplate().getInfoValues()) {
					String name = vInfo.getName();
					String sessionContextSetting = vInfo.getHandler().getSessionAssignmentClause(name, vInfo.getHandler().getValue(this, name));
					if (sessionContextSetting != null)
						sessionVariableSetStatement += (clause++ > 0 ? "," : "") + sessionContextSetting;
				}
			}
		}
		return sessionVariableSetStatement;
	}
	
	public MyPreparedStatement<String> getPreparedStatement(String key) {
		return pStmtMap.get(key);
	}
	
	public void putPreparedStatement(String key, MyPreparedStatement<String> pstmt) {
		pStmtMap.put(key, pstmt);
	}
	
	public void removePreparedStatement(String key) {
		pStmtMap.remove(key);
	}
	
	public void clearPreparedStatements() {
		pStmtMap.clear();
	}

	public void verifyWorkerGroupCleanup(WorkerGroup wg) {
		for (WorkerGroup awg : activeWG.values())
			if (awg == wg) 
				throw new PECodingException("WorkerGroup" + wg.getDisplayId() + " is still in "+getName()+" active list");
		for (WorkerGroup awg : availableWG.values())
			if (awg == wg)
				throw new PECodingException("WorkerGroup" + wg.getDisplayId() + " is still in "+getName()+" available list");
	}
	
	public ClientCapabilities getClientCapabilities() {
		return clientCapabilities;
	}
	
	public void setClientCapabilities(ClientCapabilities clientCaps) {
		this.clientCapabilities = clientCaps;
	}
		
}

