// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;

public class CompoundExpressionColumnMutator extends ColumnMutator {

	protected ExpressionNode original;
	protected CompoundExpressionRewriter rewriter;
	protected List<ExpressionPath> paths;
	
	public CompoundExpressionColumnMutator() {
		super();
	}

	public List<ExpressionNode> adapt(ExpressionNode in) {
		original = in;
		rewriter = new CompoundExpressionRewriter(original);
		ListSet<ExpressionNode> parts = rewriter.getFirstRewriteEntries();
		paths = new ArrayList<ExpressionPath>();
		ArrayList<ExpressionNode> projEntries = new ArrayList<ExpressionNode>();
		for(ExpressionNode en : parts) {
			ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(en);
			if (!cols.isEmpty()) {
				paths.add(ExpressionPath.build(en,original));
				projEntries.add(en);
			}
		}
		return projEntries;			
	}
	
	public int getNewCard() {
		return paths.size();
	}
	
	@Override
	public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ignored) {
		ExpressionNode in = getProjectionEntry(proj, getBeforeOffset());
		return adapt(in);
	}

	@Override
	public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption ignored) {
		ExpressionNode repl = (ExpressionNode) original.copy(null);
		for(int i = 0; i < paths.size(); i++) {
			ExpressionNode updated = getProjectionEntry(proj, getAfterOffsetBegin() + i);
			ExpressionPath path = paths.get(i);
			path.update(repl, updated);
		}
		return Collections.singletonList(repl);
	}
	
	
}