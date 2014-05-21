// OS_STATUS: public
package com.tesora.dve.sql.expression;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConvertFunctionCall;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

public class ExpressionUtils {
	
	public static FunctionCall buildIn(ExpressionNode lhs, List<ExpressionNode> rhs) {
		ArrayList<ExpressionNode> params = new ArrayList<ExpressionNode>();
		params.add(lhs);
		params.addAll(rhs);
		return new FunctionCall(FunctionName.makeIn(),params);
	}
	
	public static FunctionCall buildOr(List<ExpressionNode> args) {
		if (args.size() < 2)
			return null;
		return buildBinary(args,FunctionName.makeOr());
	}

	public static FunctionCall buildAnd(List<ExpressionNode> args) {
		if (args.size() < 2)
			return null;
		return buildBinary(args,FunctionName.makeAnd());
	}

	public static ExpressionNode safeBuildAnd(List<ExpressionNode> args) {
		ExpressionNode out = buildAnd(args);
		if (out == null)
			out = args.get(0);
		return out;
	}
	
	public static ExpressionNode safeBuildOr(List<ExpressionNode> args) {
		ExpressionNode out = buildOr(args);
		if (out == null)
			out = args.get(0);
		return out;
	}
	
	public static ExpressionNode buildWhereClause(List<ExpressionNode> args) {
		if (args.isEmpty())
			return null;
		else if (args.size() == 1)
			return args.get(0);
		else
			return buildAnd(args);
	}
	
	public static Pair<ColumnInstance,LiteralExpression> decomposeKeyAssignment(FunctionCall fc) {
		ColumnInstance ci = null;
		LiteralExpression le = null;
		if (fc.getFunctionName().isEquals()) {
			for(ExpressionNode e : fc.getParameters()) {
				if (e instanceof ColumnInstance)
					ci = (ColumnInstance)e;
				else if (e instanceof LiteralExpression)
					le = (LiteralExpression)e;
			}
			if (ci != null && le != null)
				return new Pair<ColumnInstance,LiteralExpression>(ci,le);
		}
		return null;
	}
	
	private static FunctionCall buildBinary(List<ExpressionNode> args, FunctionName fn) {
		if (args.size() == 2) {
			return new FunctionCall(fn, args);
		} else {
			ArrayList<ExpressionNode> params = new ArrayList<ExpressionNode>();
			params.add(args.get(0));
			ArrayList<ExpressionNode> subargs = new ArrayList<ExpressionNode>();
			for(int i = 1; i < args.size(); i++)
				subargs.add(args.get(i));
			params.add(buildBinary(subargs, fn));
			return new FunctionCall(fn, params);			
		}
	}
	
	public static ListSet<ExpressionNode> decomposeOrClause(ExpressionNode in) {
		OrDecomposer od = new OrDecomposer();
		od.visit(in);
		return od.getDecomposed();
	}
	
	public static ListSet<ExpressionNode> decomposeAndClause(ExpressionNode in) {
		AndDecomposer ad = new AndDecomposer();
		ad.visit(in);
		return ad.getDecomposed();
	}
	
	private static class OrDecomposer extends Visitor {
		
		private ListSet<ExpressionNode> acc;
		
		public OrDecomposer() {
			acc = new ListSet<ExpressionNode>(); 
		}
		
		public ListSet<ExpressionNode> getDecomposed() {
			return acc;
		}
		
		@Override
		public ExpressionNode visitFunctionCall(FunctionCall fc, VisitorContext vc) {
			if (fc.getFunctionName().isOr()) {
				return super.visitFunctionCall(fc, vc);
			} else {
				acc.add(fc);
				return fc;
			}
		}

	}
	
	private static class AndDecomposer extends Visitor {
		
		private ListSet<ExpressionNode> acc;
		
		public AndDecomposer() {
			acc = new ListSet<ExpressionNode>(); 
		}
		
		public ListSet<ExpressionNode> getDecomposed() {
			return acc;
		}
		
		@Override
		public ExpressionNode visitFunctionCall(FunctionCall fc, VisitorContext vc) {
			if (fc.getFunctionName().isAnd()) {
				return super.visitFunctionCall(fc, vc);
			} else {
				acc.add(fc);
				return fc;
			}
		}
		
	}
	
	public static FunctionCall buildConvert(ExpressionNode whatToConvert, String convertToType) {
		return new ConvertFunctionCall(whatToConvert, new UnqualifiedName(convertToType), false);
	}
	
	public static Map<RewriteKey, ExpressionNode> buildRewriteMap(Collection<ExpressionNode> in) {
		HashMap<RewriteKey, ExpressionNode> out = new HashMap<RewriteKey, ExpressionNode>();
		for(ExpressionNode n : in) {
			out.put(n.getRewriteKey(),n);
			if (n instanceof ExpressionAlias) {
				ExpressionAlias ea = (ExpressionAlias) n;
				out.put(ea.getTarget().getRewriteKey(), ea.getTarget());
			}
		}
		return out;
	}
	
	public static ExpressionNode getTarget(ExpressionNode in) {
		if (in instanceof ExpressionAlias)
			return ((ExpressionAlias)in).getTarget();
		return in;
	}	

	static public ExpressionNode getUnaryColumnInstance(ExpressionNode n) {
		ExpressionNode ci = null;
		
		if (n instanceof FunctionCall) {
			if (((FunctionCall)n).isUnaryFunction()) {
				ci = getUnaryColumnInstance(((FunctionCall) n).getParameters().get(0));
			} 
		} else {
			ci = n;
		}
		
		return ci;
	}
}
