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

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.LateBoundConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepTriggerOperation extends QueryStepOperation {

	private static final Logger logger = Logger.getLogger( QueryStepTriggerOperation.class );
	
	private final QueryStepOperation rowQuery;
	private final QueryStepOperation before;
	private final QueryStepOperation actual;
	private final QueryStepOperation after;
	
	private final TriggerValueHandlers columnHandlers;
		
	public QueryStepTriggerOperation(TriggerValueHandlers handlers,
			QueryStepOperation rowQuery, QueryStepOperation beforeBody, QueryStepOperation targetBody, QueryStepOperation afterBody) throws PEException {
		super(targetBody.getStorageGroup());
		this.rowQuery = rowQuery;
		this.before = beforeBody;
		this.after = afterBody;
		this.actual = targetBody;
		this.columnHandlers = handlers;
	}

	@Override
	public void execute(ExecutionState estate, DBResultConsumer resultConsumer) throws Throwable {
		// always
		executeRequirements(estate);

		MysqlTextResultCollector rows = new MysqlTextResultCollector(true);
		rowQuery.execute(estate, rows);
		for(List<String> row : rows.getRowData()) {
			LateBoundConstants beforeValues = columnHandlers.onBefore(estate,row);
			ExecutionState childState = estate.pushConstants(beforeValues);
			if (before != null)
				before.execute(childState, DBEmptyTextResultConsumer.INSTANCE);
			LateBoundConstants targetValues = columnHandlers.onTarget(estate,beforeValues);
			childState = estate.pushConstants(targetValues);
			estate.getValues().setRuntimeConstants(Arrays.asList(targetValues.getValues()));
			actual.execute(childState, resultConsumer);
			if (after != null) {
				LateBoundConstants afterValues = columnHandlers.onAfter(targetValues); 
				childState = estate.pushConstants(afterValues);
				after.execute(childState, DBEmptyTextResultConsumer.INSTANCE);
			}
		}
	}

	
	@Override
	public void executeSelf(ExecutionState estate, WorkerGroup wg,
			DBResultConsumer resultConsumer) throws Throwable {
		actual.executeSelf(estate, wg, resultConsumer);

	}

}
