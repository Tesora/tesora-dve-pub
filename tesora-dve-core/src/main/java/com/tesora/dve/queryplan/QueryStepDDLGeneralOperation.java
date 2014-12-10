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
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.*;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import org.apache.log4j.Logger;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepDDLGeneralOperation extends QueryStepOperation {

	private static final Logger logger = Logger.getLogger( QueryStepDDLGeneralOperation.class );

	PersistentDatabase database;
	protected DDLCallback entities;
	protected Boolean commitOverride = null;
	protected CatalogModificationExecutionStep.Action action;
	protected DistributionModel optionalModel;
	
	public QueryStepDDLGeneralOperation(StorageGroup sg, PersistentDatabase execCtxDBName) throws PEException {
		this(sg,execCtxDBName,null,null);
	}

	public QueryStepDDLGeneralOperation(StorageGroup sg, PersistentDatabase execCtxDBName, CatalogModificationExecutionStep.Action action, DistributionModel optionalModel) throws PEException {
		super((sg == null ? nullStorageGroup : sg));
		this.database = execCtxDBName;
		this.action = action;
		this.optionalModel = optionalModel;
	}

	public QueryStepDDLGeneralOperation withCommitOverride(Boolean v) {
		commitOverride = v;
		return this;
	}
	
	public void setEntities(DDLCallback entgen) {
		entities = entgen;
	}
	
	// allow derived classes to step in
	protected void prepareAction(ExecutionState estate, CatalogDAO c, WorkerGroup wg, DBResultConsumer resultConsumer) throws PEException {
		// does nothing
	}
	
	protected void postCommitAction(CatalogDAO c) throws PEException {
		entities.postCommitAction(c);
	}
	
	protected void executeAction(ExecutionState estate,CatalogDAO c, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		SQLCommand sql = entities.getCommand(c);
		try {
			if (!sql.isEmpty()) {
				WorkerRequest req = new WorkerExecuteRequest(estate.getConnection().getNonTransactionalContext(), sql).onDatabase(database);
				if (CatalogModificationExecutionStep.Action.ALTER == action && BroadcastDistributionModel.SINGLETON.equals(optionalModel)) {
					resultConsumer.setRowAdjuster(BroadcastDistributionModel.SINGLETON.getUpdateAdjuster());//replicated, need to divide row count by site count.
				} else
					resultConsumer.setRowAdjuster(RangeDistributionModel.SINGLETON.getUpdateAdjuster());//return sum of rows modified.
				wg.execute(MappingSolution.AllWorkers, req, resultConsumer);
			}
		} catch (Throwable t) {
			throw new Exception(this.getClass().getSimpleName()+".commitOverride = " + commitOverride, t);
		}
	}
	
	protected void onRollback(SSConnection conn, CatalogDAO c, WorkerGroup wg) throws PEException {
		entities.onRollback(conn,c,wg);
	}
	
	
	/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void executeSelf(ExecutionState estate, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		SSConnection ssCon = estate.getConnection();
		
		if (ssCon.hasActiveTransaction())
			throw new PEException("Cannot execute DDL within active transaction: " + entities.getCommand(ssCon.getCatalogDAO()));
		
		
		boolean noteEntities = logger.isDebugEnabled() && entities != null;

		
		// Send the catalog changes to the transaction manager so that it can
		// back them out in the event of a catastrophic failure

		// Add the catalog entries.  We have to do this before we execute any sql because the workers may
		// depend on the catalog being correct.  For instance, for a create database, the UserDatabase has to
		// exist in the catalog - otherwise the create database command is sent to the wrong database.
		CatalogDAO c = ssCon.getCatalogDAO();
		
		if (entities != null && entities.requiresFreshTxn())
			c.cleanupRollback();

		String logHeader = "("+ (entities == null ? "null" : entities.description()) + ") " + ssCon.getConnectionId() ;
		
		boolean success = false;
		boolean sideffects = false;
		CacheInvalidationRecord cacheClear = null;
		int attempts = 0;
		while(!success) {
			if (entities != null) {
				entities.beforeTxn(ssCon,c, wg);
			}
			attempts++;
			c.begin();
			try {
				List<CatalogEntity> entitiesToNotifyOfUpdate = new ArrayList<CatalogEntity>();
				List<CatalogEntity> entitiesToNotifyOfDrop = new ArrayList<CatalogEntity>();

				prepareAction(estate,c, wg, resultConsumer);

				// do the changes to the catalog first because we may be able to  
				// restore the data if the ddl operation fails on the actual database
				if (entities != null) {
					entities.inTxn(ssCon, wg);
					cacheClear = entities.getInvalidationRecord();
					QueryPlanner.invalidateCache(cacheClear);
					List<CatalogEntity> temp = entities.getUpdatedObjects();
					if (noteEntities)
						logger.debug(logHeader + " updating: " + Functional.joinToString(temp, ", "));
					for (CatalogEntity catEntity : temp) {
						c.persistToCatalog(catEntity);
						entitiesToNotifyOfUpdate.add(catEntity);
					}

					temp = entities.getDeletedObjects();
					if (noteEntities)
						logger.debug(logHeader + " deleting: " + Functional.joinToString(temp, ", "));
					for (CatalogEntity catEntity : temp) {
						if (catEntity instanceof UserDatabase) throw new IllegalArgumentException("Use drop database operation to delete a database");
						List<? extends CatalogEntity> subtemp = catEntity.getDependentEntities(c);
						if (subtemp != null) {
							if (noteEntities)
								logger.debug(logHeader + " deleting subtemp: " + Functional.joinToString(subtemp, ", "));						
							for(CatalogEntity dependentEntity : subtemp) {
								c.remove(dependentEntity);
							}
							entitiesToNotifyOfDrop.addAll(subtemp);
						}
						c.remove(catEntity);
						catEntity.removeFromParent();
						entitiesToNotifyOfDrop.add(catEntity);
					}
				}

				// TODO:
				// start a transaction with the transaction manager so that DDL can be 
				// registered to back out the DDL we are about to execute in the 
				// event of a failure after the DDL is executed but before the txn is committed.
				// or - in the case where the action succeeds but the txn fails at commit
				if (!sideffects) {
					executeAction(estate,c, wg, resultConsumer);
					sideffects = true;
					if (entities != null)
						entities.onExecute();
				}

				c.commit();
				success = true;

				if (entities != null)
					entities.onCommit(ssCon, c, wg);
				
				if (attempts > 1 || logger.isDebugEnabled())
					logger.warn("Successfully committed after " + attempts + " tries");
				
				for (CatalogEntity updatedEntity : entitiesToNotifyOfUpdate)
					updatedEntity.onUpdate();
				for (CatalogEntity deletedEntity : entitiesToNotifyOfDrop)
					deletedEntity.onDrop();
				
				
			} catch (Throwable t) {
				logger.debug(logHeader + " while executing",t);
				c.retryableRollback(t);
				onRollback(ssCon, c, wg);
				if (entities == null || !entities.canRetry(t)) {
					logger.warn(logHeader + " giving up possibly retryable ddl txn after " + attempts + " tries");
					throw new PEException(t);
				}
				// not really a warning, but it would be nice to get it back out
				logger.warn(logHeader + " retrying ddl after " + attempts + " tries upon exception: " + t.getMessage());
			} finally {
				Throwable anything = null;
				try {
					if (cacheClear != null)
						QueryPlanner.invalidateCache(cacheClear);
					cacheClear = null;
				} catch (Throwable t) {
					// throwing this away - if entities has a finally block we really want it to occur
					anything = t;
				}
				try {
					entities.onFinally(ssCon);
				} catch (Throwable t) {
					if (anything == null)
						anything = t;
				}
				if (anything != null)
					throw anything;
			}
			
			postCommitAction(c);
		}
		// Tell the transaction manager that we have executed the catalog
		// changes successfully so that they can be removed from the 
		// recovery set
	}

	@Override
	public boolean requiresTransactionSelf() {
		return false;
	}
	
	@Override
	public boolean requiresWorkers() {
		return entities.requiresWorkers();
	}

	@Override
	public boolean requiresImplicitCommit() {
		if (commitOverride != null) return commitOverride.booleanValue();
		return true;
	}
	
	public static abstract class DDLCallback {
				
		// this would be called before the txn starts
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg) throws PEException 
		{
		}
		
		// this is called after the txn starts
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException
		{
		}
		
		/*
		 * Default []
		 */
		public List<CatalogEntity> getUpdatedObjects() throws PEException
		{
			return Collections.emptyList();
		}
		
		/*
		 * Default []
		 */
		public List<CatalogEntity> getDeletedObjects() throws PEException
		{
			return Collections.emptyList();
		}
		
		// for fk support, there may be more than one command - we might have to go back and drop 
		// an existing foreign key
		public abstract SQLCommand getCommand(CatalogDAO c);
		
		/*
		 * Default cannot retry
		 */
		public boolean canRetry(Throwable t) {
			return false;
		}
		
		/*
		 * Default requires fresh txn
		 */
		public boolean requiresFreshTxn() {
			return true;
		}
		
		// used for debugging
		public abstract String description();
		
		public abstract CacheInvalidationRecord getInvalidationRecord();
		
		/*
		 * Default requires workers
		 */
		public boolean requiresWorkers() {
			return true;
		}
		
		public void postCommitAction(CatalogDAO c) throws PEException
		{
			
		}
		
		/*
		 * Called after the execute, but before the commit
		 */
		public void onExecute() {
			
		}
		
		/*
		 * Called after the commit, but before any notifies
		 */
		public void onCommit(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			
		}
		
		/*
		 * Called for every rollback
		 */
		public void onRollback(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			
		}
		
		/*
		 * Always called
		 */
		public void onFinally(SSConnection conn) throws Throwable {
			
		}
	}
	
	@Override
	public String toString() {
		return new StringBuffer(this.getClass().getSimpleName()+"(db="+database+",commitOverride="+commitOverride+"):"+entities.description()).toString();
	}
}
