package com.tesora.dve.sql.transform.strategy.aggregation;

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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.GroupConcatCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.ApplyOption;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.PassThroughMutator;
import com.tesora.dve.sql.transform.strategy.ProjectionMutator;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;

public class AggFunProjectionMutator extends ProjectionMutator {

	public AggFunProjectionMutator(SchemaContext sc) {
		super(sc);
	}

	@Override
	public List<ExpressionNode> adapt(MutatorState ms, List<ExpressionNode> proj) {
		for(int i = 0; i < proj.size(); i++) {
			ExpressionNode actual = ColumnMutator.getProjectionEntry(proj, i);
			ColumnMutator cm = null;
			if (EngineConstant.AGGFUN.has(actual)) {
				FunctionCall fc = (FunctionCall) actual;
				FunctionName fn = fc.getFunctionName();
				if (fn.isMax()) {
					cm = new MaxMutator();
				} else if (fn.isMin()) {
					cm = new MinMutator();
				} else if (fn.isAverage()) {
					cm = new AvgMutator();
				} else if (fn.isCount()) {
					cm = new CountMutator();
				} else if (fn.isSum()) {
					cm = new SummingMutator();
				} else if (fn.isGroupConcat()) {
					cm = new GroupConcatMutator();
				} else if (fn.isVariance()) {
					cm = new VarianceMutator();
				} else {
					throw new SchemaException(Pass.PLANNER, "Unknown agg fun kind: " + fc);
				}
			} else {
				cm = new PassThroughMutator();
			}
			cm.setBeforeOffset(i);
			columns.add(cm);
		}
		List<ExpressionNode> out = applyAdapted(proj, ms);
		AggregationMutatorState ams = (AggregationMutatorState) ms;
		ams.clearGroupBys();
		return out;
	}

	public List<ColumnMutator> getAggColumns() {
		return Functional.select(columns, new UnaryPredicate<ColumnMutator>() {

			@Override
			public boolean test(ColumnMutator object) {
				return object instanceof AggFunMutator;
			}
			
		});
	}
	
	public boolean requiresPlainFirstPass() {
		return Functional.any(columns, new UnaryPredicate<ColumnMutator>() {

			@Override
			public boolean test(ColumnMutator object) {
				if (object instanceof PassThroughMutator) return false;
				AggFunMutator agf = (AggFunMutator)object;
				return agf.requiresNoGroupingFirstPass();
			}
			
		});
	}
	
	protected static class AvgMutator extends AggFunMutator {

		protected AvgMutator() {
			super(FunctionName.makeAvg());
		}

		@Override
		public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
			ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
			FunctionCall avg = (FunctionCall) getProjectionEntry(proj, getBeforeOffset());
			quantifier = avg.getSetQuantifier();
			ExpressionNode param = avg.getParametersEdge().get(0);
			ExpressionNode sumParam = (ExpressionNode) param.copy(null);
			ExpressionNode countParam= (ExpressionNode) param.copy(null);
			out.add(sumParam);
			out.add(countParam);
			return out;
		}

		@Override
		public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
			ExpressionNode first = getProjectionEntry(proj, getAfterOffsetBegin());
			ExpressionNode second = getProjectionEntry(proj, getAfterOffsetBegin() + 1);
			if (opts.getMaxSteps() == 2) {
				// 2.1 - sum(x), count(y)
				// 2.2 - sum(a)/sum(b)
				if (opts.getCurrentStep() == 1) {
					FunctionCall sum = new FunctionCall(FunctionName.makeSum(), first);
					sum.setSetQuantifier(quantifier);
					FunctionCall count = new FunctionCall(FunctionName.makeCount(), second);
					count.setSetQuantifier(quantifier);
					ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
					out.add(sum);
					out.add(count);
					return out;
				}
				FunctionCall sum = new FunctionCall(FunctionName.makeSum(),first);
				FunctionCall count = new FunctionCall(FunctionName.makeSum(),second);
				FunctionCall avg = new FunctionCall(FunctionName.makeDivide(),sum,count);
				avg.setGrouped();
				return Collections.singletonList((ExpressionNode)avg);
			}
			// 1.1 - sum(x)/count(y)
			FunctionCall sum = new FunctionCall(FunctionName.makeSum(),first);
			sum.setSetQuantifier(quantifier);
			FunctionCall count = new FunctionCall(FunctionName.makeCount(),first);
			count.setSetQuantifier(quantifier);
			FunctionCall avg = new FunctionCall(FunctionName.makeDivide(),sum,count);
			avg.setGrouped();
			return Collections.singletonList((ExpressionNode)avg);
		}
		
	}
	
	protected static class VarianceMutator extends AggFunMutator {

		private final Map<AggFunMutator, ExpressionNode> children = new LinkedHashMap<AggFunMutator, ExpressionNode>() {
			private static final long serialVersionUID = -4636984875883190733L;

			{
				put(new AvgMutator(), null);
			}
		};

		protected VarianceMutator() {
			super(FunctionName.makeVariance());
		}

		@Override
		public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
			final List<ExpressionNode> out = new ArrayList<ExpressionNode>();
			final FunctionCall var = (FunctionCall) getProjectionEntry(proj, getBeforeOffset());
			quantifier = var.getSetQuantifier();

			final ExpressionNode param = var.getParametersEdge().get(0);
			final ExpressionNode countParam = (ExpressionNode) param.copy(null);
			final ExpressionNode varParam = (ExpressionNode) param.copy(null);
			final ExpressionNode dAvg2Param = (ExpressionNode) param.copy(null);
			out.add(countParam);
			out.add(varParam);
			out.add(dAvg2Param);

			return out;
		}

		@Override
		public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
			final ExpressionNode countParam = getProjectionEntry(proj, getAfterOffsetBegin());
			final ExpressionNode varParam = getProjectionEntry(proj, getAfterOffsetBegin() + 1);
			final ExpressionNode dAvg2Param = getProjectionEntry(proj, getAfterOffsetBegin() + 2);
			final ExpressionNode gu = this.children.values().iterator().next();

			if (opts.getMaxSteps() == 2) {
				// 2.1 - count(x) n_i, avg(x) u_i, var_*(x) s2_i, dAvg2
				if (opts.getCurrentStep() == 1) {
					final FunctionCall n_i = new FunctionCall(FunctionName.makeCount(), countParam);
					n_i.setSetQuantifier(quantifier);

					final FunctionCall s2_i = new FunctionCall(FunctionName.makeVariance(), varParam);
					s2_i.setSetQuantifier(quantifier);

					final FunctionCall u_i = new FunctionCall(FunctionName.makeAvg(), dAvg2Param);
					final FunctionCall u_i_minus_gu = new FunctionCall(FunctionName.makeMinus(), u_i, gu);
					final FunctionCall dAvg2 = new FunctionCall(FunctionName.makePow(), u_i_minus_gu, LiteralExpression.makeLongLiteral(2));
					dAvg2.setSetQuantifier(quantifier);

					final List<ExpressionNode> out = new ArrayList<ExpressionNode>();
					out.add(n_i);
					out.add(s2_i);
					out.add(dAvg2);
					return out;
				}

				// 2.2 -
				final FunctionCall n_i_s2_i = new FunctionCall(FunctionName.makeMult(), countParam, varParam);
				final FunctionCall sum_n_i = new FunctionCall(FunctionName.makeSum(), countParam);

				final FunctionCall sum_n_i_s2_i = new FunctionCall(FunctionName.makeSum(), n_i_s2_i);
				final FunctionCall n_i_dAvg2 = new FunctionCall(FunctionName.makeMult(), countParam, dAvg2Param);
				final FunctionCall sum_n_i_dAvg2 = new FunctionCall(FunctionName.makeSum(), n_i_dAvg2);

				final FunctionCall term_1 = new FunctionCall(FunctionName.makeDivide(), sum_n_i_s2_i, sum_n_i);
				final FunctionCall term_2 = new FunctionCall(FunctionName.makeDivide(), sum_n_i_dAvg2, sum_n_i);

				final FunctionCall total = new FunctionCall(FunctionName.makePlus(), term_1, term_2);

				total.setGrouped();
				return Collections.singletonList((ExpressionNode) total);
			}

			// TODO
			return null;
		}

		@Override
		public Map<AggFunMutator, ExpressionNode> getChildren() {
			return this.children;
		}

	}

	protected static class MaxMutator extends RecursingMutator {
		protected MaxMutator() {
			super(FunctionName.makeMax());
		}
	}

	protected static class MinMutator extends RecursingMutator {
		protected MinMutator() {
			super(FunctionName.makeMin());
		}
	}

	protected static class CountMutator extends AggFunMutator {

		private Wildcard wc;

		protected CountMutator() {
			super(FunctionName.makeCount());
		}

		@Override
		public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
			ExpressionNode param = getProjectionEntry(proj, getBeforeOffset());
			FunctionCall fc = (FunctionCall) param;
			quantifier = fc.getSetQuantifier();
			List<ExpressionNode> out = new ArrayList<ExpressionNode>();
			ExpressionNode arg = fc.getParametersEdge().get(0);
			if (arg instanceof Wildcard) {
				// count(*) - turn this into count(pk) for a base table
				wc = (Wildcard) arg;
				// we're going to say the standin is any unique column that is a base table
				for(FromTableReference ftr : ms.getStatement().getTablesEdge()) {
					if (ftr.getBaseTable() != null) {
						PEAbstractTable<?> pet = ftr.getBaseTable().getAbstractTable();
						List<PEKey> yuks = (pet.isView() ? Collections.<PEKey>emptyList() : pet.asTable().getUniqueKeys(sc)); 
						if (!yuks.isEmpty())
							out.add(new ColumnInstance(yuks.get(0).getColumns(sc).get(0),ftr.getBaseTable()));
						else {
							// use the first nonnull column
							for(PEColumn pec : pet.getColumns(sc)) {
								if (pec.isNotNullable()) {
									out.add(new ColumnInstance(pec,ftr.getBaseTable()));
									break;
								}
							}
						}
					}
				}
				if (out.isEmpty())
					out.add(LiteralExpression.makeLongLiteral(1));
			} else {
				out.addAll(fc.getParameters());
			}
			return out;
		}

		@Override
		public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
			FunctionCall fc = null;
			if (opts.getMaxSteps() == 2) {
				if (opts.getCurrentStep() == 1) {
					if (wc == null)
						fc = new FunctionCall(fn, getProjectionEntries(proj, getAfterOffsetBegin(), getAfterOffsetEnd()));
					else
						fc = new FunctionCall(fn, wc);
					fc.setSetQuantifier(quantifier);
				} else {
					fc = new FunctionCall(FunctionName.makeSum(), getProjectionEntry(proj, getAfterOffsetBegin()));
					if (fn.isCount())
						fc = ExpressionUtils.buildConvert(fc, "SIGNED");						
				}
			} else {
				if (wc == null)
					fc = new FunctionCall(fn,getProjectionEntries(proj, getAfterOffsetBegin(), getAfterOffsetEnd()));
				else
					fc = new FunctionCall(fn,wc);
				fc.setSetQuantifier(quantifier);
			}
			return Collections.singletonList((ExpressionNode)fc);				
		}

		
	}
	
	protected static class SummingMutator extends AggFunMutator {

		
		protected SummingMutator() {
			super(FunctionName.makeSum());
		}

		@Override
		public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
			ExpressionNode param = getProjectionEntry(proj, getBeforeOffset());
			FunctionCall fc = (FunctionCall) param;
			quantifier = fc.getSetQuantifier();
			ExpressionNode arg = fc.getParametersEdge().get(0);
			return Collections.singletonList(arg);
		}

		@Override
		public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
			ExpressionNode param = getProjectionEntry(proj, getAfterOffsetBegin());
			FunctionCall fc = null;
			if (opts.getMaxSteps() == 2) {
				if (opts.getCurrentStep() == 1) {
					fc = new FunctionCall(fn,param);
					fc.setSetQuantifier(quantifier);
				} else {
					fc = new FunctionCall(FunctionName.makeSum(), param);
				}
			} else {
				fc = new FunctionCall(fn,param);
				fc.setSetQuantifier(quantifier);
			}
			return Collections.singletonList((ExpressionNode)fc);				
		}
		
	}
	
	protected static class GroupConcatMutator extends AggFunMutator {

		// group concat - we do the columns only in the first pass
		// and then reapply the concat afterwards

		private String separator;
		
		protected GroupConcatMutator() {
			super(FunctionName.makeGroupConcat());
		}
		
		@Override
		public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
			ExpressionNode param = getProjectionEntry(proj, getBeforeOffset());
			GroupConcatCall fc = (GroupConcatCall) param;
			separator = fc.getSeparator();
			return fc.getParameters();
		}

		@Override
		public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
			if (opts.getMaxSteps() == opts.getCurrentStep()) {
				ExpressionNode param = getProjectionEntry(proj, getAfterOffsetBegin());
				GroupConcatCall gcc = new GroupConcatCall(fn, Collections.singletonList(param),null,separator);
				return Collections.singletonList((ExpressionNode)gcc);				
			}
			return getSingleColumn(proj, getAfterOffsetBegin());
		}
		
		@Override
		public boolean requiresNoGroupingFirstPass() {
			return true;
		}
		
	}
}
