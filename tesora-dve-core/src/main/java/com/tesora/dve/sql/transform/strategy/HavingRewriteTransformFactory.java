// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;


import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.AggFunCollector;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

/*
 * Applies if the query has a having clause
 */
public class HavingRewriteTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.HAVING;
	}

	private static class HavingState extends Traversal {
		
		private ExpressionNode originalHavingClause;
		private ListOfPairs<ExpressionNode, ExpressionPath> paths;
		private ListOfPairs<ExpressionNode, Integer> offsets;
		private ListSet<ExpressionNode> found;
		private int origProjSize;
		
		public HavingState(SchemaContext sc, ExpressionNode orig, SelectStatement ss) {
			super(Order.PREORDER, ExecStyle.ONCE);
			originalHavingClause = orig;
			found = new ListSet<ExpressionNode>();
			paths = new ListOfPairs<ExpressionNode, ExpressionPath>();
			offsets = new ListOfPairs<ExpressionNode, Integer>();
			origProjSize = ss.getProjectionEdge().size();
			traverse(orig);
			Map<RewriteKey,ExpressionNode> projEntryByKey = ExpressionUtils.buildRewriteMap(ss.getProjection());
			boolean mustNormalize = false;
			for(ExpressionNode en : found) {
				ExpressionNode actual = null;
				boolean isAlias = false;
				if (en instanceof AliasInstance) {
					AliasInstance ai = (AliasInstance) en;
					actual = ai.getTarget();
					isAlias = true;
				} else {
					actual = en;
				} 
				ExpressionPath path = ExpressionPath.build(en, originalHavingClause);
				RewriteKey rk = actual.getRewriteKey();
				int offset = -1;
				ExpressionNode inplace = projEntryByKey.get(rk);
				if (inplace == null) {					
					// must be added
					if (isAlias)
						throw new SchemaException(Pass.PLANNER, "Unable to find alias within projection");
					offset = ss.getProjectionEdge().size();
					ss.getProjectionEdge().add((ExpressionNode)en.copy(null));
					mustNormalize = true;
				} else {
					for(int i = 0; i < ss.getProjectionEdge().size(); i++) {
						ExpressionAlias pen = (ExpressionAlias) ss.getProjectionEdge().get(i);
						if (pen.getRewriteKey().equals(rk)) {
							offset = i;
							break;
						} else if (pen.getTarget().getRewriteKey().equals(rk)) {
							offset = i;
							break;
						}
					}
					if (offset == -1)
						throw new SchemaException(Pass.PLANNER, "Unable to find offset within projection");
				}
				paths.add(en,path);
				offsets.add(en,offset);
			}
			if (mustNormalize)
				ss.normalize(sc);
		}

		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.AGGFUN.has(in) || EngineConstant.COLUMN.has(in) || EngineConstant.ALIAS_INSTANCE.has(in)) {
				// don't add if any parent is already in the added (recall this is preorder)
				boolean any = false;
				for(LanguageNode n : getPath()) {
					if (found.contains(n)) {
						any = true;
						break;
					}
				}
				if (!any)
					found.add((ExpressionNode)in);
			}
			return in;
		}

		public void adjust(SelectStatement ss) {
			// so, first pull off anything that was add by us - that by definition is everything after the
			// original proj offset
			HashMap<ExpressionNode, ExpressionNode> byOrig = new HashMap<ExpressionNode,ExpressionNode>();
			// any synthetic entries we're going to remove.  
			for(Pair<ExpressionNode, Integer> po : offsets) {
				Integer off = po.getSecond();
				ExpressionAlias ith = (ExpressionAlias) ss.getProjectionEdge().get(off.intValue()); 
				if (off.intValue() >= origProjSize) {
					byOrig.put(po.getFirst(), ith.getTarget());
				} else {
					byOrig.put(po.getFirst(), ith.buildAliasInstance());
				}
			}
			// truncate the projection
			while(ss.getProjectionEdge().size() > origProjSize) 
				ss.getProjectionEdge().remove(ss.getProjectionEdge().size() - 1);
			// the having clause might have used aliases, so we're going to modify the original
			for(Pair<ExpressionNode, ExpressionPath> pa : paths) {
				ExpressionNode updated = byOrig.get(pa.getFirst());
				ExpressionPath path = pa.getSecond();
				path.update(originalHavingClause, updated);
			}
			ss.setHavingExpression(originalHavingClause);
		}
		
	}
	
	private static class AliasExpander extends Traversal {

		public AliasExpander() {
			super(Order.POSTORDER, ExecStyle.ONCE);
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.ALIAS_INSTANCE.has(in)) {
				AliasInstance ai = (AliasInstance) in;
				ExpressionAlias targ = ai.getTarget();
				return (LanguageNode) targ.getTarget().copy(null);
			}
			return in;
		}
		
	}
	
	@Override
	public FeatureStep plan(DMLStatement statement, PlannerContext ipc) throws PEException {
		if (!EngineConstant.HAVINGCLAUSE.has(statement))
			return null;

		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		SelectStatement in = (SelectStatement) statement;
		SelectStatement copy = (SelectStatement)CopyVisitor.copy(in);

		// if this is a having clause with no agg funs in the projection or in the having expression,
		// convert to a where clause.
		
		ListSet<FunctionCall> projAggFun = EngineConstant.PROJ_AGGREGATE_FUNCTIONS.getValue(copy,context.getContext());
		ListSet<FunctionCall> havingAggFuns = AggFunCollector.collectAggFuns(copy.getHavingExpression());
		
		if ((projAggFun == null || projAggFun.isEmpty()) &&
				(havingAggFuns == null || havingAggFuns.isEmpty())) {
			// where clause case
			// decompose the where clause, add the having clause as a clause
			ListSet<ExpressionNode> parts = ExpressionUtils.decomposeAndClause(copy.getWhereClause());
			ExpressionNode hc = copy.getHavingExpression();
			hc = (ExpressionNode) new AliasExpander().traverse(hc);
			parts.add(copy.getHavingExpression());
			copy.getHavingEdge().clear();
			if (parts.size() < 2)
				copy.setWhereClause(parts.get(0));
			else
				copy.setWhereClause(ExpressionUtils.buildAnd(parts));
			
			// plan the child as normal
			return buildPlan(copy, context.withTransform(getFeaturePlannerID()), DefaultFeaturePlannerFilter.INSTANCE);
		} 

		// normal case
		HavingState hs = new HavingState(context.getContext(),copy.getHavingExpression(), copy);
		copy.getHavingEdge().clear();
							
		ProjectingFeatureStep childPlan = 
				(ProjectingFeatureStep) buildPlan(copy,context.withTransform(getFeaturePlannerID()),DefaultFeaturePlannerFilter.INSTANCE);

		SelectStatement ss = (SelectStatement) childPlan.getPlannedStatement();
		hs.adjust(ss);

		return childPlan;
	}
}
