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

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.ILateEvalLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.transform.CopyContext;

public class LateEvaluatingLiteralExpression extends DelegatingLiteralExpression implements ILateEvalLiteralExpression {

	private final LateEvaluator evaluator;
	private final ConstantExpression[] params;
	
	public LateEvaluatingLiteralExpression(int literalType, ConstantExpression[] parameters, LateEvaluator eval) {
		super(literalType,null,null,0,null,true);
		evaluator = eval;
		params = parameters;
		if (params.length != evaluator.getParamTypes().length)
			throw new SchemaException(Pass.PLANNER, "LateEvaluatingLiteralExpression: expect " + evaluator.getParamTypes().length + " params but have " + params.length);
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ConstantExpression[] np = new ConstantExpression[params.length];
		for(int i = 0; i < params.length; i++)
			np[i] = (ConstantExpression) params[i].copy(cc);
		LateEvaluatingLiteralExpression out = new LateEvaluatingLiteralExpression(getValueType(),np,evaluator);
		return out;
	}

	
	@Override
	public Object getValue(ConnectionValues cv) {
		IConstantExpression[] vals = new IConstantExpression[params.length];
		for(int i = 0; i < params.length; i++)
			vals[i] = params[i];
		return evaluator.getValue(cv, vals);
	}
	
	public static abstract class LateEvaluator {
		
		private final Class<?>[] paramTypes;
		
		public LateEvaluator(Class<?>[] expected) {
			this.paramTypes = expected;
		}
		
		public Class<?>[] getParamTypes() {
			return paramTypes;
		}
		
		public abstract Object compute(Object[] in);
		
		public Object getValue(ConnectionValues cv, IConstantExpression[] params) {
			Object[] converted = new Object[params.length];
			for(int i = 0; i < paramTypes.length; i++) {
				Object v = params[i].getValue(cv);
				converted[i] = convert(v,paramTypes[i]);
			}
			return compute(converted);
		}
		
		protected Object convert(Object in, Class<?> c) {
			if (c.isInstance(in))
				return in;
			if (Number.class.isAssignableFrom(c)) {
				if (in instanceof Number) {
					Number n = (Number) in;
					if (Long.class.equals(c)) {
						return Long.valueOf(n.longValue());
					} else if (Integer.class.equals(c)) {
						return Integer.valueOf(n.intValue());
					}
				} else if (in instanceof String) {
					String s = (String) in;
					if (Long.class.equals(c)) {
						return Long.valueOf(s);
					} else if (Integer.class.equals(c)) {
						return Integer.valueOf(s);
					}
				}
			} 
			throw new SchemaException(Pass.PLANNER, "Fill me in: conversion from " + in.getClass().getSimpleName() + " to " + c.getSimpleName() + " in LateEvaluatingLiteralExpression");
		}
	}

	public static final LateEvaluator SUM = new LateEvaluator(new Class<?>[] { Long.class, Long.class }) {

		@Override
		public Object compute(Object[] in) {
			Long l = (Long) in[0];
			Long r = (Long) in[1];
			return new Long(l.longValue() + r.longValue());
		}
		
	};

	@Override
	public ILiteralExpression getCacheExpression() {
		IConstantExpression[] cacheParams = new IConstantExpression[params.length];
		for(int i = 0; i < cacheParams.length; i++) {
			cacheParams[i] = params[i].getCacheExpression();
		}
		return new CachedLateEvaluatingLiteralExpression(getValueType(),evaluator,cacheParams);
	}

	@Override
	public int getPosition() {
		return 0;
	}

}
