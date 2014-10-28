package com.tesora.dve.queryplan;

import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;

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

public abstract class QueryStepModifyTimestampVariableOperation extends
		QueryStepOperation {

	long timestampVariableValue = 0;

	public QueryStepModifyTimestampVariableOperation(StorageGroup sg) throws PEException {
		super(sg);
	}
	
	public long getTimestampVariableValue() {
		return timestampVariableValue;
	}

	public void setTimestampVariableValue(long timestampVariableValue) {
		this.timestampVariableValue = timestampVariableValue;
	}
	
}
