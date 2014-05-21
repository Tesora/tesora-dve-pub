// OS_STATUS: public
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


import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;

public class CachedActualLiteralExpression implements ILiteralExpression {

	private final Object value;
	private final int type;

	public CachedActualLiteralExpression(int valueType, Object value) {
		this.type = valueType;
		this.value = value;
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		return value;
	}

	@Override
	public int getPosition() {
		return 0;
	}

	@Override
	public boolean isParameter() {
		return false;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return this;
	}

	@Override
	public boolean isNullLiteral() {
		return type == TokenTypes.NULL;
	}

	@Override
	public boolean isStringLiteral() {
		return type == TokenTypes.Character_String_Literal;
	}

	@Override
	public int getValueType() {
		return type;
	}

	@Override
	public UnqualifiedName getCharsetHint() {
		return null;
	}

}
