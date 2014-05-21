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
