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



import com.tesora.dve.sql.node.expression.LateEvaluatingLiteralExpression.LateEvaluator;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.ILateEvalLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;

public class CachedLateEvaluatingLiteralExpression implements
		ILateEvalLiteralExpression {

	private final int type;
	private final LateEvaluator evaluator;
	private final IConstantExpression[] expressions;
	
	public CachedLateEvaluatingLiteralExpression(int type, LateEvaluator eval, IConstantExpression[] params) {
		this.type = type;
		evaluator = eval;
		expressions = params;
	}
	
	@Override
	public int getPosition() {
		return 0;
	}

	@Override
	public ILiteralExpression getCacheExpression() {
		return this;
	}

	@Override
	public boolean isNullLiteral() {
		return false;
	}

	@Override
	public boolean isStringLiteral() {
		return this.type == TokenTypes.Character_String_Literal;
	}

	@Override
	public int getValueType() {
		return this.type;
	}

	@Override
	public UnqualifiedName getCharsetHint() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(ConnectionValues cv) {
		return evaluator.getValue(cv, expressions);	
	}

	@Override
	public ConstantType getConstantType() {
		return ConstantType.LITERAL;
	}

}
