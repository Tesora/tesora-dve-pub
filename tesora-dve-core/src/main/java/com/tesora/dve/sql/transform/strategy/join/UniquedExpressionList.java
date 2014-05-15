// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;

public class UniquedExpressionList {
	
	private List<ExpressionNode> exprs;
	private Set<RewriteKey> keys;
	
	public UniquedExpressionList() {
		exprs = new ArrayList<ExpressionNode>();
		keys = new HashSet<RewriteKey>();
	}

	public List<ExpressionNode> getExprs() {
		return exprs;
	}
	
	public UniquedExpressionList addAll(Collection<ExpressionNode> c) {
		if (c == null) return this;
		for(ExpressionNode en : c)
			add(en);
		return this;
	}
	
	public UniquedExpressionList add(ExpressionNode en) {
		if (en == null) return this;
		RewriteKey rk = en.getRewriteKey();
		if (keys.add(rk))
			exprs.add(en);
		return this;
	}
}