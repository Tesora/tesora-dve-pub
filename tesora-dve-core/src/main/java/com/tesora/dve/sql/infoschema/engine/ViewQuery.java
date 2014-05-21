// OS_STATUS: public
package com.tesora.dve.sql.infoschema.engine;

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

import java.util.Map;

import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class ViewQuery {

	// query using the view schema
	protected SelectStatement query;
	// null if query is against multiple tables
	protected TableInstance singleTable;
	// map of named parameters.  if parameters are used in the query they will be NamedParameter instances
	protected Map<String, Object> params;
	// flag to override any privilege and root checks
	protected Boolean overrideRequiresPrivilegeValue = null;
	
	public ViewQuery(SelectStatement q, Map<String,Object> args, TableInstance onTable) {
		this(q, args, onTable, null);
	}
	
	public ViewQuery(SelectStatement q, Map<String,Object> args, TableInstance onTable, Boolean overrideRequiresPrivilegeValue) {
		query = q;
		params = args;
		singleTable = onTable;
		this.overrideRequiresPrivilegeValue = overrideRequiresPrivilegeValue;
	}
	
	public SelectStatement getQuery() {
		return query;
	}
	
	public Map<String,Object> getParams() {
		return params;
	}
	
	public TableInstance getTable() {
		return singleTable;
	}

	public Boolean getoverrideRequiresPrivilegeValue() {
		return overrideRequiresPrivilegeValue;
	}

	public void setoverrideRequiresPrivilegeValue(Boolean overrideRequiresPrivilegeValue) {
		this.overrideRequiresPrivilegeValue = overrideRequiresPrivilegeValue;
	}
	
}
