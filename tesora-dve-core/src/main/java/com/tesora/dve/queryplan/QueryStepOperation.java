package com.tesora.dve.queryplan;

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
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.common.logutil.LogSubject;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerManager;

/**
 * Baseclass of <b>QueryStep</b> operations, which include, but are not limited to 
 * {@link QueryStepInsertByKeyOperation},
 * {@link QueryStepSelectAllOperation} and {@link QueryStepRedistOperation}.
 * @author mjones
 *
 */
public abstract class QueryStepOperation implements LogSubject {

    static final Logger logger = Logger.getLogger(QueryStepOperation.class);
	
	protected final List<QueryStepOperation> reqs;
	protected final StorageGroup sg;
	
	public QueryStepOperation(StorageGroup sg) throws PEException {
		if (sg == null)
			throw new PEException("Invalid QSO construction: null storage group");
		this.sg = sg;
		this.reqs = new ArrayList<QueryStepOperation>();
	}
	
	public void addRequirement(QueryStepOperation qso) {
		if (qso == this)
			throw new IllegalArgumentException("Invalid required step: self");
		reqs.add(qso);
	}

	public void execute(ExecutionState state, DBResultConsumer resultConsumer) throws Throwable {
		executeRequirements(state);
		executeOperation(state,resultConsumer);
	}
	
	final void executeRequirements(ExecutionState estate) throws Throwable {
		for(QueryStepOperation qso : reqs)
			qso.execute(estate, DBEmptyTextResultConsumer.INSTANCE);
	}

	void executeOperation(ExecutionState execState, DBResultConsumer resultConsumer) throws Throwable {
		SSConnection ssCon = execState.getConnection();
		try {
			ExecutionLogger beforeLogger = ssCon.getExecutionLogger().getNewLogger("BeforeStepExec");
			WorkerGroup wg = null;
			if (requiresImplicitCommit())
				ssCon.implicitCommit();
			if (requiresWorkers()) {
				if (sg == nullStorageGroup)
					throw new PEException("No storage group specified but one required");
				wg = ssCon.getWorkerGroupAndPushContext(sg,getContextDatabase());
			}
			ExecutionLogger slowQueryLogger = null;
			try {
				beforeLogger.end();
				if (logger.isDebugEnabled())
					logger.debug("QueryStep executes " + toString());
				slowQueryLogger = ssCon.getExecutionLogger().getNewLogger(this); 

				executeSelf(execState, wg, resultConsumer);
			} finally {
				ExecutionLogger afterLogger = null;
				try {
					slowQueryLogger.end();
					afterLogger = ssCon.getExecutionLogger().getNewLogger("AfterStepExec");
				} finally {
					try {
						if (wg != null)
							ssCon.returnWorkerGroupAndPopContext(wg);
					} finally {
						if (afterLogger != null)
							afterLogger.end();
					}
				}
			}
		} finally {
		}
	}

	protected SQLCommand bindCommand(ExecutionState in, SQLCommand sqlc) {
		return sqlc.getLateResolvedCommand(in);
	}
	
	/**
	 * Called by <b>QueryStep</b> to execute the operation.  Any results
	 * of the operation will be returned in an instance of {@link ResultCollector}.
	 * 
	 * @param ssCon user's connection
	 * @param wg provisioned {@link WorkerGroup} against which to execute the step
	 * @param resultConsumer 
	 * @return {@link ResultCollector} with results, or <b>null</b>
	 * @throws JMSException
	 * @throws PEException
	 * @throws Throwable 
	 */
	public abstract void executeSelf(ExecutionState execState, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable;
	
	public boolean requiresTransactionSelf() {
		return true;
	}
	
	boolean requiresTransaction() {
		if (requiresTransactionSelf())
			return true;
		return Functional.any(reqs, new UnaryPredicate<QueryStepOperation>() {

			@Override
			public boolean test(QueryStepOperation object) {
				return object.requiresTransactionSelf();
			}
			
		});
	}

	public boolean hasRequirements() {
		return !reqs.isEmpty();
	}

	
	
	/**
	 * Does this query step require workers?  Default is true.  Some DDL operations are catalog-only, and
	 * thus do not require workers.
	 */
	public boolean requiresWorkers() {
		return true;
	}
	
	/**
	 * Returns true if the QueryStepOperation causes any inflight transactions to be committed
	 * first.
	 */
	public boolean requiresImplicitCommit() {
		return false;
	}	

	@Override
	public String describeForLog() {
		return this.getClass().getSimpleName();
	}
	
	public PersistentDatabase getContextDatabase() {
		return null;
	}
	
	public final StorageGroup getStorageGroup() {
		return sg;
	}
	
	
	protected static final StorageGroup nullStorageGroup = new StorageGroup() {

		@Override
		public String getName() {
			fail();
			return null;
		}

		@Override
		public int sizeForProvisioning() throws PEException {
			fail();
			return 0;
		}

		@Override
		public void provisionGetWorkerRequest(GetWorkerRequest getWorkerRequest)
				throws PEException {
			fail();
		}

		@Override
		public void returnWorkerSites(WorkerManager workerManager,
				Collection<? extends StorageSite> groupSites)
				throws PEException {
			fail();
		}

		@Override
		public boolean isTemporaryGroup() {
			fail();
			return false;
		}

		@Override
		public int getId() {
			fail();
			return 0;
		}

		private void fail() throws IllegalStateException {
			throw new IllegalStateException("Use of null storage group");
		}
		
	};

	// this is the ONLY place we should be calling mapping from
	public static WorkerGroup.MappingSolution mapKeyForInsert(ExecutionState estate, StorageGroup wg, IKeyValue inkey) throws PEException {
		return inkey.getDistributionModel().mapKeyForInsert(estate.getCatalogDAO(), wg, bindKey(estate,inkey));
	}
	
	private static IKeyValue bindKey(ExecutionState estate, IKeyValue ikv) throws PEException {
		return (estate.getBoundConstants().isEmpty() ? ikv : ikv.rebind(estate.getBoundConstants()));
	}
	
	public static WorkerGroup.MappingSolution mapKeyForUpdate(ExecutionState estate, StorageGroup wg, IKeyValue inkey) throws PEException {		
		return inkey.getDistributionModel().mapKeyForUpdate(estate.getCatalogDAO(), wg, bindKey(estate,inkey));
	}

	public static WorkerGroup.MappingSolution mapKeyForQuery(ExecutionState estate, StorageGroup wg, IKeyValue key, DistKeyOpType operation) throws PEException {
		return key.getDistributionModel().mapKeyForQuery(
				estate.getCatalogDAO(), wg, bindKey(estate,key),
				operation);
	}

	public static WorkerGroup.MappingSolution mapKeyForQuery(ExecutionState estate, StorageGroup wg, IKeyValue key, 
			DistributionModel givenDistribution, DistKeyOpType operation) throws PEException {
		return givenDistribution.mapKeyForQuery(
				estate.getCatalogDAO(), wg, bindKey(estate,key),
				operation);
	}
}