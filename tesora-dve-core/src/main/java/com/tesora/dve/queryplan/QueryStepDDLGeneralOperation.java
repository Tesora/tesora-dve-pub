// OS_STATUS: public
package com.tesora.dve.queryplan;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.UserDatabase;
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
	
	public QueryStepDDLGeneralOperation(PersistentDatabase execCtxDBName) {
		this.database = execCtxDBName;
	}

	public QueryStepDDLGeneralOperation withCommitOverride(Boolean v) {
		commitOverride = v;
		return this;
	}
	
	public void setEntities(DDLCallback entgen) {
		entities = entgen;
	}
		
	// allow derived classes to step in
	protected void prepareAction(CatalogDAO c) throws PEException {
		// does nothing
	}
	
	protected void postCommitAction(CatalogDAO c) throws PEException {
		entities.postCommitAction(c);
	}
	
	protected void executeAction(SSConnection ssCon,CatalogDAO c, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		SQLCommand sql = entities.getCommand(c);
		try {
			if (!sql.isEmpty()) {
				WorkerRequest req = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), sql).onDatabase(database);
				wg.execute(MappingSolution.AllWorkers, req, resultConsumer);
			}
		} catch (Throwable t) {
			throw new Exception(this.getClass().getSimpleName()+".commitOverride = " + commitOverride, t);
		}
	}
	
	protected void onRollback() throws PEException {
		
	}
	
	
	/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {

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

				prepareAction(c);

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
					executeAction(ssCon,c, wg, resultConsumer);
					sideffects = true;
				}

				c.commit();
				success = true;
				
				if (attempts > 1 || logger.isDebugEnabled())
					logger.warn("Successfully committed after " + attempts + " tries");
				
				for (CatalogEntity updatedEntity : entitiesToNotifyOfUpdate)
					updatedEntity.onUpdate();
				for (CatalogEntity deletedEntity : entitiesToNotifyOfDrop)
					deletedEntity.onDrop();
			} catch (Throwable t) {
				logger.debug(logHeader + " while executing",t);
				c.retryableRollback(t);
				onRollback();
				if (entities == null || !entities.canRetry(t)) {
					logger.warn(logHeader + " giving up possibly retryable ddl txn after " + attempts + " tries");
					throw new PEException(t);
				}
				// not really a warning, but it would be nice to get it back out
				logger.warn(logHeader + " retrying ddl after " + attempts + " tries upon exception: " + t.getMessage());
			} finally {
				if (cacheClear != null)
					QueryPlanner.invalidateCache(cacheClear);
				cacheClear = null;
			}
			
			postCommitAction(c);
		}
		// Tell the transaction manager that we have executed the catalog
		// changes successfully so that they can be removed from the 
		// recovery set
	}

	@Override
	public boolean requiresTransaction() {
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
	
	public interface DDLCallback {
		
		// this would be called before the txn starts
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg) throws PEException;
		
		// this is called after the txn starts
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException;
		
		public List<CatalogEntity> getUpdatedObjects() throws PEException;
		
		public List<CatalogEntity> getDeletedObjects() throws PEException;
		
		// for fk support, there may be more than one command - we might have to go back and drop 
		// an existing foreign key
		public SQLCommand getCommand(CatalogDAO c);
		
		public boolean canRetry(Throwable t);
		
		public boolean requiresFreshTxn();
		
		// used for debugging
		public String description();
		
		public CacheInvalidationRecord getInvalidationRecord();
		
		public boolean requiresWorkers();
		
		public void postCommitAction(CatalogDAO c) throws PEException;
	}
	
	@Override
	public String toString() {
		return new StringBuffer(this.getClass().getSimpleName()+"(db="+database+",commitOverride="+commitOverride+"):"+entities.description()).toString();
	}
}
