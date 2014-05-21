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

import com.tesora.dve.common.catalog.PersistentDatabase;


public abstract class QueryStepResultsOperation extends QueryStepDMLOperation {

	public static final long NO_LIMIT = -1;
	
	long resultsLimit = Long.MAX_VALUE;

	protected QueryStepResultsOperation(PersistentDatabase pdb) {
		super(pdb);
	}
	
	protected long getResultsLimit() {
		return resultsLimit;
	}

	public QueryStepResultsOperation setResultsLimit(long resultsLimit) {
		this.resultsLimit = resultsLimit;
		return this;
	}
	
	public static boolean recordCountSatisfiesLimit(long recordCount, long resultsLimit) {
		return resultsLimit != NO_LIMIT && recordCount >= resultsLimit; 
	}

	public static boolean recordCountViolatesLimit(long recordCount, long resultsLimit) {
		return resultsLimit != NO_LIMIT && recordCount > resultsLimit; 
	}

	public static int rowsToReturn(long recordCount, long resultsLimit) {
		long rowsToReturn = recordCount;
		if (resultsLimit != NO_LIMIT && recordCount > resultsLimit)
			rowsToReturn = Math.max(0, resultsLimit - recordCount);
		return rowsToReturn < Integer.MAX_VALUE ? (int)rowsToReturn : Integer.MAX_VALUE;
	}

}
