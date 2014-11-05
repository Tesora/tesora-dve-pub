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

import com.tesora.dve.db.ValueConverter;
import com.tesora.dve.sql.schema.types.Type;

public class TriggerValueHandler {

	protected final Type type;
	
	public TriggerValueHandler(Type t) {
		this.type = t;
	}
	
	public Object onBefore(ExecutionState estate, String value) {
		if (value == null) return null;
		return ValueConverter.INSTANCE.convert(value, type);
	}
	
	// if you override this, make sure you override the has as well
	public Object onTarget(ExecutionState estate, Object beforeValue) {
		return beforeValue;
	}
	
	public Object onAfter(Object targetValue) {
		return targetValue;
	}
	
	
	public boolean hasTarget() {
		return false;
	}
	
	public boolean hasAfter() {
		return false;
	}
	
}
