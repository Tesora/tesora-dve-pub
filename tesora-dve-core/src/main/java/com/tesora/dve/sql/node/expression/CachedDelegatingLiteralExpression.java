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
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;

public class CachedDelegatingLiteralExpression implements IDelegatingLiteralExpression {

	protected int tokenType;
	protected int position;
	protected UnqualifiedName charsetHint;
	
	public CachedDelegatingLiteralExpression(int type, int offset, UnqualifiedName cshint) {
		this.tokenType = type;
		this.position = offset;
		this.charsetHint = cshint;
	}
	
	@Override
	public UnqualifiedName getCharsetHint() {
		return charsetHint;
	}
	
	@Override
	public boolean isNullLiteral() {
		return tokenType == TokenTypes.NULL;
	}

	@Override
	public boolean isStringLiteral() {
		return tokenType == TokenTypes.Character_String_Literal;
	}

	@Override
	public Object getValue(ConnectionValues cv) {
		return cv.getLiteral(this);
	}

	@Override
	public int getPosition() {
		return position;
	}

	@Override
	public int getValueType() {
		return tokenType;
	}

	@Override
	public ILiteralExpression getCacheExpression() {
		return this;
	}

	@Override
	public ConstantType getConstantType() {
		return ConstantType.LITERAL;
	}

}
