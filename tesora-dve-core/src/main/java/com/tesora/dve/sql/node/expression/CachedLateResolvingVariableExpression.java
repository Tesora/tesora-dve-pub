package com.tesora.dve.sql.node.expression;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.variables.AbstractVariableAccessor;

public class CachedLateResolvingVariableExpression implements IConstantExpression {

	private final AbstractVariableAccessor accessor;
	
	public CachedLateResolvingVariableExpression(AbstractVariableAccessor va) {
		accessor = va;
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		try {
			return accessor.getValue(sc.getConnection().getVariableSource());
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to obtain variable value",pe);
		}
	}

	@Override
	public int getPosition() {
		return -1;
	}

	@Override
	public boolean isParameter() {
		return false;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return this;
	}

}
