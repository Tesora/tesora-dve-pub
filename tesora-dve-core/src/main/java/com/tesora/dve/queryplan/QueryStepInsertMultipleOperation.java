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

import java.util.Collection;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepInsertMultipleOperation extends QueryStepDMLOperation {
	
	static Logger logger = Logger.getLogger( QueryStepInsertMultipleOperation.class );

	DistributionModel distModel;
	Collection<Entry<SQLCommand, KeyValue>> insertList;
	
	public QueryStepInsertMultipleOperation(UserDatabase database,
			DistributionModel distModel, Collection<Entry<SQLCommand, KeyValue>> insertList) {
		super(database);
		this.distModel = distModel;
		this.insertList = insertList;
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		beginExecution();
		for (Entry<SQLCommand, KeyValue> entry : insertList) {
			QueryStepInsertByKeyOperation
					.executeInsertByKey(ssCon, wg, resultConsumer, database, entry.getKey(),
							entry.getValue());
		}
		endExecution(resultConsumer.getUpdateCount());
	}
}
