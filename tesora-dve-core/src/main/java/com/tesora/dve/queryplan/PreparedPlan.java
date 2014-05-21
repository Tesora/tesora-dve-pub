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
