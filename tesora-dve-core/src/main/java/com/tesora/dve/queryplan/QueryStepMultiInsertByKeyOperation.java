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
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageGroup;
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
	
	public QueryStepMultiInsertByKeyOperation(StorageGroup sg, PersistentDatabase execCtxDBName) throws PEException {
		super(sg, execCtxDBName);
	}
	
	public void addStatement(IKeyValue key, SQLCommand sql) throws PEException {
		if (sql.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		insertList.add(new Pair<IKeyValue, SQLCommand>(key, sql));
	}

	@Override
	public void executeSelf(ExecutionState estate, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {

		MultiMap<StorageSite, SQLCommand> mappedInserts = new MultiMap<StorageSite, SQLCommand>();

		SSConnection ssCon = estate.getConnection();
		
		// note that all the key/cmd pairs in the insertList must be of the same DistributionModel
		DistributionModel dm = insertList.get(0).getFirst().getDistributionModel();
		for(Pair<IKeyValue, SQLCommand> keyCmd : insertList) {
			if ( dm != keyCmd.getFirst().getDistributionModel() ) {
				throw new PEException("Distribution models in multi-insert operations must be the same. Started with " + dm + 
						" but found " + keyCmd.getFirst().getDistributionModel());
			}
			
			WorkerGroup.MappingSolution mappingSolution = dm.mapKeyForInsert(ssCon.getCatalogDAO(), wg.getGroup(), keyCmd.getFirst());
			mappedInserts.put(wg.resolveSite(mappingSolution.getSite()), bindCommand(estate,keyCmd.getSecond()));
		}

		WorkerRequest req = new WorkerMultiInsertRequest(ssCon.getTransactionalContext(), mappedInserts).onDatabase(database);
		
		resultConsumer.setRowAdjuster(dm.getInsertAdjuster());
		beginExecution();
		WorkerGroup.syncWorkers(wg.submit(WorkerGroup.MappingSolution.AllWorkers, req, resultConsumer, insertList.size()));
		endExecution(resultConsumer.getUpdateCount());
	}

}
