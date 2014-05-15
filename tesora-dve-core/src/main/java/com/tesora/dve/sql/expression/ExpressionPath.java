// OS_STATUS: public
package com.tesora.dve.sql.expression;


import java.util.LinkedList;
import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;

public final class ExpressionPath {

	private List<Edge<?,?>> steps;
	private ExpressionPath(List<Edge<?,?>> p) {
		steps = p;
	}
	
	public LanguageNode apply(LanguageNode in) {
		LanguageNode p = in;
		for(Edge<?,?> e : steps) {
			LanguageNode o = e.apply(p);
			if (o == null) return null;
			p = o;
		}
		return p;
	}
	
	public int size() {
		return steps.size();
	}

	@SuppressWarnings("unchecked")
	public <T extends LanguageNode> void update(LanguageNode in, T repl) {
		T target = (T) apply(in);
		Edge<?,T> parentEdge = target.getParentEdge();
		parentEdge.set(repl);
	}
	
	public boolean has(UnaryPredicate<Edge<?,?>> predicate) {
		return Functional.any(steps, predicate);
	}
	
	public boolean has(final EdgeTest et) {
		return has(new UnaryPredicate<Edge<?,?>>() {

			@Override
			public boolean test(Edge<?, ?> object) {
				return object.getName().baseMatches(et.getName());
			}
			
		});
	}
	
	public static ExpressionPath build(LanguageNode from, LanguageNode to) {
		LinkedList<Edge<?,?>> path = new LinkedList<Edge<?,?>>();
		LanguageNode p = from;
		while (p != to && p != null) {
			Edge<?,?> parentEdge = p.getParentEdge();
			path.add(0, parentEdge);
			if (parentEdge == null)
				// went too far
				return null;
			p = parentEdge.getParent();
		}
		if (path.isEmpty())
			throw new SchemaException(Pass.PLANNER, "Built empty expression path");
		return new ExpressionPath(path);
	}	
}
