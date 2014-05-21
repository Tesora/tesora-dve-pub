// OS_STATUS: public
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

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepFilterOperation extends QueryStepOperation {

	QueryStepOperation source;
	OperationFilter filter;
	
	public QueryStepFilterOperation(QueryStepOperation src, OperationFilter filter) {
		super();
		this.source = src;
		this.filter = filter;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		MysqlTextResultCollector tempRc = new MysqlTextResultCollector(true);
		source.execute(ssCon, wg, tempRc);
		filter.filter(ssCon, tempRc.getColumnSet(), tempRc.getRowData(), tempRc);
	}

	@Override
	public boolean requiresTransaction() {
		return source.requiresTransaction();
	}
	
	@Override
	public boolean requiresWorkers() {
		return source.requiresWorkers();
	}

	@Override
	public boolean requiresImplicitCommit() {
		return source.requiresImplicitCommit();
	}	

	public interface OperationFilter {
		
		/**
		 * @param ssCon - input - reference to relevant SSConnection
		 * @param columnSet - input - column metadata
		 * @param rowData - input - list of row data packets
		 * @param results - output - this will be filled in by the filter method
		 * @throws Throwable
		 */
		public void filter(SSConnection ssCon, ColumnSet columnSet, 
				List<ArrayList<String>> rowData, DBResultConsumer results) throws Throwable;

		// for testing
		public String describe();
		
	}
	
}
