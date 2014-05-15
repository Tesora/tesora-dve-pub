// OS_STATUS: public
package com.tesora.dve.sql.node.expression;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.transform.CopyContext;

public class ExpressionSet extends ExpressionNode {

	private MultiEdge<ExpressionSet, ExpressionNode> values = 
			new MultiEdge<ExpressionSet, ExpressionNode>(ExpressionSet.class, this, EdgeName.EXPRESSION_SET_VALUE);

	public ExpressionSet(final List<ExpressionNode> values, final SourceLocation origin) {
		super(origin);
		this.values.set(values);
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		final List<ExpressionNode> copyValues = new ArrayList<ExpressionNode>(this.values.size());
		for (final ExpressionNode value : this.getSubExpressions()) {
			copyValues.add((ExpressionNode) value.copy(cc));
		}
		return new ExpressionSet(copyValues, getSourceLocation());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?, ?>> List<T> getEdges() {
		return (List<T>) Collections.singletonList(this.values);
	}

	@Override
	public List<ExpressionNode> getSubExpressions() {
		return this.values.getMulti();
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return true;
	}

	@Override
	protected int selfHashCode() {
		return 0;
	}

}
