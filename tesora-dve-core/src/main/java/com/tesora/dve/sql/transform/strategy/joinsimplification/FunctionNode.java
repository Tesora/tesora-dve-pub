package com.tesora.dve.sql.transform.strategy.joinsimplification;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;

public class FunctionNode extends NRNode {

	List<NRNode> children;
	FunctionName fn;
	ExpressionNode simplified;
	
	public FunctionNode(SchemaContext sc, FunctionCall fc, List<NRNode> chilluns) {
		super(fc);
		fn = fc.getFunctionName();
		children = chilluns;
		simplified = buildSimplified(sc);
	}
	
	public ExpressionNode getSimplifiedValue() {
		return simplified;
	}
	
	@Override
	public boolean required(TableKey tab) {
		if (fn.isAnd())
			return any(tab);
		else if (fn.isOr())
			return computeOrRequired(tab);
		else if (fn.isIs())
			return computeIsRequired(tab);
		else if (fn.isIfNull())
			// be conservative
			return false;
		else 
			return any(tab);
	}

	@Override
	protected Set<TableKey> computeUses() {
		HashSet<TableKey> acc = new HashSet<TableKey>();
		for(NRNode nrn : children)
			acc.addAll(nrn.uses());
		return acc;
	}

	private boolean any(TableKey tab) {
		for(NRNode nrn : children) {
			if (nrn.required(tab))
				return true;
		}
		return false;
	}
	
	private boolean computeOrRequired(TableKey tab) {
		// required only iff every branch says it is
		for(NRNode nrn : children) {
			if (!nrn.required(tab))
				return false;
		}
		return true;
	}
	
	private boolean computeIsRequired(TableKey tab) {
		ExpressionNode rhs = ((FunctionCall)wrapping).getParametersEdge().get(1);
		if (rhs instanceof LiteralExpression) {
			LiteralExpression litex = (LiteralExpression) rhs;
			if (litex.isNullLiteral())
				return false;
		}
		return (children.get(0).required(tab));
	}
	
	private ExpressionNode buildSimplified(SchemaContext sc) {
		// we can only simplifiy if all params are literals and we understand the function
		FunctionCall fc = (FunctionCall) wrapping;
		GeneralFunctionSimplifier gfs = simplifiers.get(fn.getTokenID());
		if (gfs != null)
			return gfs.compute(sc, fc.getParameters());
		return null;
	}

	private static abstract class GeneralFunctionSimplifier {

		protected String handles;
		public GeneralFunctionSimplifier(String fn) {
			handles = fn;
		}
		
		public Number asNumber(SchemaContext sc, ExpressionNode en) {
			if (en instanceof LiteralExpression) {
				LiteralExpression l = (LiteralExpression) en;
				Object o = l.getValue(sc);
				if (o instanceof Number)
					return (Number)o;
			}
			return null;
		}
		
		public Boolean asBoolean(SchemaContext sc, ExpressionNode en) {
			if (en instanceof LiteralExpression) {
				LiteralExpression l = (LiteralExpression) en;
				Object o = l.getValue(sc);
				if (o instanceof Boolean)
					return (Boolean)o;
			}
			return null;
		}

		public abstract ExpressionNode compute(SchemaContext sc, List<ExpressionNode> params);
	}
	
	private static abstract class FunctionSimplifier extends GeneralFunctionSimplifier {
		
		public FunctionSimplifier(String fn) {
			super(fn);
		}
		
		abstract ExpressionNode computeResult(SchemaContext sc, ExpressionNode litex, ExpressionNode ritex);
		
		public ExpressionNode compute(SchemaContext sc, List<ExpressionNode> params) {
			if (params.isEmpty())
				return null;
			return computeResult(sc, params.get(0), params.get(1));
		}
		
	}

	private static final FunctionSimplifier equalsOperator = new FunctionSimplifier("=") {

		@Override
		public ExpressionNode computeResult(SchemaContext sc, ExpressionNode litex,
				ExpressionNode ritex) {
			Number ln = asNumber(sc,litex);
			Number rn = asNumber(sc,ritex);
			if (ln != null && rn != null)
				return LiteralExpression.makeBooleanLiteral(ln.equals(rn));
			return null;
		}
		
	};

	private static final FunctionSimplifier notEqualsOperator = new FunctionSimplifier("<>") {

		@Override
		public ExpressionNode computeResult(SchemaContext sc, ExpressionNode litex,
				ExpressionNode ritex) {
			Number ln = asNumber(sc,litex);
			Number rn = asNumber(sc,ritex);
			if (ln != null && rn != null)
				return LiteralExpression.makeBooleanLiteral(!ln.equals(rn));
			return null;
		}
		
	};
	

	
	private static final FunctionSimplifier andOperator = new FunctionSimplifier("and") {

		@Override
		ExpressionNode computeResult(SchemaContext sc, ExpressionNode litex,
				ExpressionNode ritex) {
			Boolean lb = asBoolean(sc,litex);
			Boolean rb = asBoolean(sc,ritex);
			if (lb != null && rb == null) { 
				if (lb.booleanValue())
					return ritex;
				else
					return LiteralExpression.makeBooleanLiteral(false);
			} else if (lb == null && rb != null) {
				if (rb.booleanValue())
					return litex;
				else
					return LiteralExpression.makeBooleanLiteral(false);
			} else if (lb != null && rb != null) {
				return LiteralExpression.makeBooleanLiteral(lb.booleanValue() && rb.booleanValue());
			} else {
				return null;
			}
		}

		
	};
	
	private static final FunctionSimplifier orOperator = new FunctionSimplifier("or") {

		@Override
		ExpressionNode computeResult(SchemaContext sc, ExpressionNode litex,
				ExpressionNode ritex) {
			Boolean lb = asBoolean(sc,litex);
			Boolean rb = asBoolean(sc,ritex);
			if (lb != null && rb == null) {
				if (lb.booleanValue())
					return LiteralExpression.makeBooleanLiteral(true);
				else
					return ritex;
			} else if (lb == null && rb != null) {
				if (rb.booleanValue())
					return LiteralExpression.makeBooleanLiteral(true);
				else
					return litex;
			} else if (lb != null && rb != null) {
				return LiteralExpression.makeBooleanLiteral(lb.booleanValue() || rb.booleanValue());
			} else {
				return null;
			}
		}

		
	};

	private static final GeneralFunctionSimplifier ifFunction = new GeneralFunctionSimplifier("if") {

		@Override
		public ExpressionNode compute(SchemaContext sc,
				List<ExpressionNode> params) {
			// orig if(test,true-expr,false-expr)
			// if true-expr == false-expr, then we can reduce this down to true-expr
			ExpressionNode trueExpr = params.get(1);
			ExpressionNode falseExpr = params.get(2);
			RewriteKey lrk = trueExpr.getRewriteKey();
			RewriteKey rrk = falseExpr.getRewriteKey();
			if (lrk.equals(rrk))
				return trueExpr;
			return null;
		}
		
	};
	
	private static final Map<Integer, GeneralFunctionSimplifier> simplifiers = buildSimplifiers();
	
	private static Map<Integer,GeneralFunctionSimplifier> buildSimplifiers() {
		HashMap<Integer,GeneralFunctionSimplifier> out = new HashMap<Integer,GeneralFunctionSimplifier>();
		out.put(TokenTypes.Equals_Operator, equalsOperator);		
		out.put(TokenTypes.Not_Equals_Operator, notEqualsOperator); 
		out.put(TokenTypes.AND, andOperator); 
		out.put(TokenTypes.OR, orOperator);
		out.put(TokenTypes.IF, ifFunction);
		return out;
	}
}
