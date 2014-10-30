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

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.IParameter;

public class CachedParameterExpression implements IParameter {

	private int position;
	
	public CachedParameterExpression(int pos) {
		position = pos;
	}

	@Override
	public Object getValue(SchemaContext sc) {
		return sc.getValueManager().getValue(sc, this);
	}

	@Override
	public int getPosition() {
		return position;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return this;
	}

	@Override
	public ConstantType getConstantType() {
		return ConstantType.LITERAL;
	}
	
}
