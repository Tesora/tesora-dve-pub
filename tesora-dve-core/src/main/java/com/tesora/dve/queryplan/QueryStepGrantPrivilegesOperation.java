// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.Priviledge;
import com.tesora.dve.common.catalog.Tenant;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.WorkerGrantPrivilegesRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepGrantPrivilegesOperation extends QueryStepOperation {

	static Logger logger = Logger.getLogger( QueryStepCreateDatabaseOperation.class );
		
	private Priviledge priv;
	private CacheInvalidationRecord invalidationRecord;
	
	public QueryStepGrantPrivilegesOperation(Priviledge p, CacheInvalidationRecord cir) {
		this.priv = p;
		this.invalidationRecord = cir;
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
		// this op is only called for non-global privileges, so figure out which database it's on
		
		UserDatabase database = priv.getDatabase();
		Tenant tenant = priv.getTenant();
		if (tenant != null)
			database = tenant.getDatabase();
		User user = priv.getUser();
		
		if (ssCon.hasActiveTransaction())
            throw new PEException("Cannot execute DDL within active transaction: GRANT PRIVILEDGES ON " + database.getName() + ".* TO " +
                    Singletons.require(HostService.class).getDBNative().getUserDeclaration(user, false));
		
		CatalogDAO c = ssCon.getCatalogDAO();
		c.begin();
		try {
			// if the user is new, persist that as well
			if (user.getId() == 0)
				c.persistToCatalog(user);
			else
				c.refresh(user);
			
			c.persistToCatalog(priv);
			
			// TODO:
			// start a transaction with the transaction manager so that DDL can be 
			// registered to back out the DDL we are about to execute in the 
			// event of a failure after the DDL is executed but before the txn is committed.

            WorkerRequest req = new WorkerGrantPrivilegesRequest(ssCon.getNonTransactionalContext(), database.getName(), Singletons.require(HostService.class).getDBNative().getUserDeclaration(user, true));
			wg.execute(MappingSolution.AllWorkers, req, resultConsumer);
			
			c.commit();
		} catch (Throwable t) {
			c.rollback(t);
			throw t;
		} finally {
			// regardless of whether it worked or not, bump the ddl counter so that we get refreshes on contexts
			QueryPlanner.invalidateCache(invalidationRecord);
		}

		// Tell the transaction manager that we have executed the catalog
		// changes successfully so that they can be removed from the 
		// recovery set
		
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}

}
