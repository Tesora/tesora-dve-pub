// OS_STATUS: public
package com.tesora.dve.queryplan;

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
