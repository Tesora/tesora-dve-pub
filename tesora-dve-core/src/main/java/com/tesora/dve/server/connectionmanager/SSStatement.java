// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlan;

public class SSStatement {
	
	static Logger logger = Logger.getLogger( SSStatement.class );

	static int statementCounter = 0;

	String statementId;
	
	QueryPlan queryPlan;

	SSStatement() {
        statementId = Singletons.require(HostService.class).getHostName() + ++statementCounter;
	}

	public String getId() {
		return statementId;
	}

	public void setQueryPlan(QueryPlan plan) throws PEException {
		clearQueryPlan();
		queryPlan = plan;
	}
	
	public void close() throws PEException {
		clearQueryPlan();
	}
	
	public QueryPlan getQueryPlan() {
		return queryPlan;
	}

	public void clearQueryPlan() throws PEException {
		if (queryPlan != null) {
			queryPlan.close();
			queryPlan = null;
		}
	}
}
