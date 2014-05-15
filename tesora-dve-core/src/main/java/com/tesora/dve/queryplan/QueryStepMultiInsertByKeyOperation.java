// OS_STATUS: public
package com.tesora.dve.queryplan;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepMultiInsertByKeyOperation extends QueryStepDMLOperation {

	static Logger logger = Logger.getLogger( QueryStepMultiInsertByKeyOperation	.class );

	List<Pair<IKeyValue, SQLCommand>> insertList = new ArrayList<Pair<IKeyValue, SQLCommand>>();
	
	public QueryStepMultiInsertByKeyOperation(PersistentDatabase execCtxDBName) {
		super(execCtxDBName);
	}
	
	public void addStatement(IKeyValue key, SQLCommand sql) throws PEException {
		if (sql.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		insertList.add(new Pair<IKeyValue, SQLCommand>(key, sql));
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {

		MultiMap<StorageSite, SQLCommand> mappedInserts = new MultiMap<StorageSite, SQLCommand>();

		// note that all the key/cmd pairs in the insertList must be of the same DistributionModel
		DistributionModel dm = insertList.get(0).getFirst().getDistributionModel();
		for(Pair<IKeyValue, SQLCommand> keyCmd : insertList) {
			if ( dm != keyCmd.getFirst().getDistributionModel() ) {
				throw new PEException("Distribution models in multi-insert operations must be the same. Started with " + dm + 
						" but found " + keyCmd.getFirst().getDistributionModel());
			}
			
			WorkerGroup.MappingSolution mappingSolution = dm.mapKeyForInsert(ssCon.getCatalogDAO(), wg.getGroup(), keyCmd.getFirst());
			mappedInserts.put(wg.resolveSite(mappingSolution.getSite()), keyCmd.getSecond());
		}

		WorkerRequest req = new WorkerMultiInsertRequest(ssCon.getTransactionalContext(), mappedInserts).onDatabase(database);
		
		resultConsumer.setRowAdjuster(dm.getInsertAdjuster());
		beginExecution();
		WorkerGroup.syncWorkers(wg.submit(WorkerGroup.MappingSolution.AllWorkers, req, resultConsumer, insertList.size()));
		endExecution(resultConsumer.getUpdateCount());
	}

}
