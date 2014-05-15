// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.parser.CandidateParser;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;

public class CandidateCachedPlan {

	private final ExecutionPlan ep;
	private final boolean cacheIfNotFound;
	private final CandidateParser shrunk;
	
	public CandidateCachedPlan(CandidateParser cp, ExecutionPlan ep, boolean encache) {
		this.ep = ep;
		this.shrunk = cp;
		this.cacheIfNotFound = encache;
	}
	
	public ExecutionPlan getPlan() { return ep; }
	public boolean tryCaching() { return cacheIfNotFound; }
	public CandidateParser getShrunk() { return shrunk; }
	
}
