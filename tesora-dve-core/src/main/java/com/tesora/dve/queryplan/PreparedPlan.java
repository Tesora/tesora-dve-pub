// OS_STATUS: public
package com.tesora.dve.queryplan;

public class PreparedPlan {

	QueryPlan metadataPlan;
	String prepareSQL;
	Object unboundPlan;
	
	public PreparedPlan(QueryPlan metadataPlan, String psql, Object unbound) {
		this.metadataPlan = metadataPlan;
		this.prepareSQL = psql;
		this.unboundPlan = unbound;
	}
	
	public QueryPlan getMetadataPlan() {
		return metadataPlan;
	}
	
	public String getOriginalSQL() {
		return prepareSQL;
	}
	
	public Object getUnboundPlan() {
		return unboundPlan;
	}	
}
