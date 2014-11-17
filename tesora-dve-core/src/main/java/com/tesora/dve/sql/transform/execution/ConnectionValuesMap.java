package com.tesora.dve.sql.transform.execution;

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

import java.util.LinkedHashMap;
import java.util.Map;

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.SchemaContext;

// due to nested plans, there could be more than one set of connection values
// for a plan.
public class ConnectionValuesMap {

	private final LinkedHashMap<ExecutionPlan,ConnectionValues> values;
	
	public ConnectionValuesMap() {
		values = new LinkedHashMap<ExecutionPlan,ConnectionValues>();
	}

	public void addValues(ExecutionPlan ep, ConnectionValues cv) {
		ConnectionValues any = values.put(ep,cv);
		if (any != null)
			throw new SchemaException(Pass.PLANNER, "Duplicate connection values for plan " + ep);
		if (cv == null)
			throw new SchemaException(Pass.PLANNER, "Null conn values");
	}
	
	public void take(ConnectionValuesMap other) {
		values.putAll(other.values);
	}
	
	public ConnectionValues getValues(ExecutionPlan ep) {
		return values.get(ep);
	}
	
	public ConnectionValues getRootValues() {
		for(Map.Entry<ExecutionPlan,ConnectionValues> me : values.entrySet()) {
			if (me.getKey().isRoot())
				return me.getValue();
		}
		return null;
	}
	
	// testing constructor
	public ConnectionValuesMap(RootExecutionPlan root, SchemaContext sc) {
		this();
		addValues(root,sc.getValues());
		root.collectNonRootValueTemplates(sc, this);
	}
}
