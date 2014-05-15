// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.ILateEvalLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.transform.CopyContext;

public class LateEvaluatingLiteralExpression extends DelegatingLiteralExpression implements ILateEvalLiteralExpression {

	private final LateEvaluator evaluator;
	private final ConstantExpression[] params;
	
	public LateEvaluatingLiteralExpression(int literalType, ValueSource vm, ConstantExpression[] parameters, LateEvaluator eval) {
		super(literalType,null,vm,0,null);
		evaluator = eval;
		params = parameters;
		if (params.length != evaluator.getParamTypes().length)
			throw new SchemaException(Pass.PLANNER, "LateEvaluatingLiteralExpression: expect " + evaluator.getParamTypes().length + " params but have " + params.length);
	}
	
	@Override
	public boolean isParameter() {
		return false;
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ConstantExpression[] np = new ConstantExpression[params.length];
		for(int i = 0; i < params.length; i++)
			np[i] = (ConstantExpression) params[i].copy(cc);
		LateEvaluatingLiteralExpression out = new LateEvaluatingLiteralExpression(getValueType(),source,np,evaluator);
		return out;
	}

	
	@Override
	public Object getValue(SchemaContext sc) {
		IConstantExpression[] vals = new IConstantExpression[params.length];
		for(int i = 0; i < params.length; i++)
			vals[i] = params[i];
		return evaluator.getValue(sc, vals);
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
		
		public Object getValue(SchemaContext sc, IConstantExpression[] params) {
			Object[] converted = new Object[params.length];
			for(int i = 0; i < paramTypes.length; i++) {
				Object v = params[i].getValue(sc);
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
