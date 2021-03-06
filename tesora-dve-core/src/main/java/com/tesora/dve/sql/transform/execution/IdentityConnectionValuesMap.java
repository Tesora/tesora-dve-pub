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

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.schema.ConnectionValues;

public class IdentityConnectionValuesMap extends ConnectionValuesMap {

	private final ConnectionValues single;
	
	public IdentityConnectionValuesMap(ConnectionValues cv) {
		super();
		single = cv;
	}
	
	public ConnectionValues getValues(ExecutionPlan ep) {
		return single;
	}
	
	public ConnectionValues getRootValues() {
		return single;
	}
	
	public void addValues(ExecutionPlan ep, ConnectionValues cv) {
		throw new SchemaException(Pass.PLANNER, "Invalid call to IdentityConnectionValuesMap.addValues");
	}
}
