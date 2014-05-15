// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.joinsimplification;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.jg.UncollapsedJoinGraph;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.FunCollector;
import com.tesora.dve.sql.util.ListSet;


/*
 * For a filter such as
 * where p.a = 1 and p.a = q.a
 * we transform this to
 * where p.a = 1 and q.a = 1
 */
public class ConstantFoldingSimplifier extends Simplifier {

	@Override
	public boolean applies(SchemaContext sc, DMLStatement dmls) throws PEException {
		return dmls instanceof MultiTableDMLStatement && EngineConstant.WHERECLAUSE.has(dmls);
	}

	@Override
	public DMLStatement simplify(SchemaContext sc, DMLStatement in, JoinSimplificationTransformFactory parent) throws PEException {
		String before = (parent.emitting() ? in.getSQL(sc) : null);
		CopyContext cc = in.getMapper().getCopyContext();
		ExpressionNode wc = (ExpressionNode) EngineConstant.WHERECLAUSE.get(in);
		List<ExpressionNode> andClauses = ExpressionUtils.decomposeAndClause(wc);
		// so for each and clause in turn, look for eq funs; also or clauses
		// build up a map of column=constant and column=column, and note column=column
		// at the end see if there column=constant for one side of those
		// if we find an or clause in one of the branches, ignore the whole branch.
		MultiMap<ColumnKey,RewriteKey> matching = new MultiMap<ColumnKey,RewriteKey>();
		List<FunctionCall> eqjs = new ArrayList<FunctionCall>();
		for(ExpressionNode c : andClauses) {
			ListSet<FunctionCall> calls = FunCollector.collectFuns(c);
			if (calls == null || calls.isEmpty()) continue;
			boolean bad = false;
			for(LanguageNode ln : calls) {
				FunctionCall fc = (FunctionCall) ln;
				if (fc.getFunctionName().isOr() || fc.getFunctionName().isNot()) {
					bad = true;
					break;
				}
			}
			if (bad) continue;
			for(LanguageNode ln : calls) {
				FunctionCall fc = (FunctionCall) ln;
				if (fc.getFunctionName().isEquals()) {
					ExpressionNode lhs = fc.getParametersEdge().get(0);
					ExpressionNode rhs = fc.getParametersEdge().get(1);
					ColumnKey lk = null;
					if (lhs instanceof ColumnInstance) {
						ColumnInstance ci = (ColumnInstance) lhs;
						lk = ci.getColumnKey();
					}
					if (lk != null) {
						RewriteKey rk = null;
						if (rhs instanceof ColumnInstance) {
							rk = rhs.getRewriteKey();
							eqjs.add(fc);
							matching.put(lk,rk);
							matching.put((ColumnKey)rk, lk);
						} else if (rhs instanceof LiteralExpression) {
							rk = rhs.getRewriteKey();
							matching.put(lk,rk);
						}
					}
				}
			}
		}
		if (eqjs.isEmpty())
			return null;
		// we have equijoins in the where clause, go figure out the raw joins
		UncollapsedJoinGraph ujg = new UncollapsedJoinGraph(sc,(MultiTableDMLStatement) in);
		boolean any = false;
		for(FunctionCall fc : eqjs) {
			ColumnInstance rc = (ColumnInstance) fc.getParametersEdge().get(1);
			// figure out if the rhs is mapped to something else
			ColumnKey lookup = rc.getColumnKey();
			Collection<RewriteKey> sub = matching.get(lookup);
			if (sub == null || sub.isEmpty()) 
				continue;
			// look for a constant
			for(RewriteKey rk : sub) {
				if (rk instanceof ColumnKey) continue;
				// at this point we know we could possibly rewrite the expression - so let's make sure we actually can
				// we can if for both sides removing the edge that this eq embodies doesn't completely disconnect the partition
				// from the graph.  if it would completely disconnect the partition - then we will replace p.a = q.a with
				// p.a = <constant> and p.a = q.a
				// this should produce better partition queries
				ColumnInstance lc = (ColumnInstance) fc.getParametersEdge().get(0);
				TableKey ltk = lc.getTableInstance().getTableKey();
				TableKey rtk = rc.getTableInstance().getTableKey();
				DPart lpart = ujg.getPartitionFor(ltk);
				DPart rpart = ujg.getPartitionFor(rtk);
				boolean additive = (lpart.getEdges().size() == 1 || rpart.getEdges().size() == 1);
				FunctionCall fcopy = null;
				if (additive) 
					fcopy = (FunctionCall)fc.copy(cc);
				ExpressionNode orig = (ExpressionNode) rk.toInstance();
				// make a copy of the node
				ExpressionNode copy = (ExpressionNode) orig.copy(cc);
				// replace
				rc.getParentEdge().set(copy);
				if (additive) {
					Edge<?, LanguageNode> parentEdge = fc.getParentEdge();
					FunctionCall both = new FunctionCall(FunctionName.makeAnd(),fcopy,fc);
					parentEdge.set(both);
				}
				any = true;
				fc.getBlock().clear();
			}
			
		}
		if (any) {
			if (parent.emitting()) {
				parent.emit(" CF in:  " + before);
				parent.emit(" CF out: " + in.getSQL(sc));
			}
			wc.getBlock().clear();
			return in;
		}
		return null;
	}
}
