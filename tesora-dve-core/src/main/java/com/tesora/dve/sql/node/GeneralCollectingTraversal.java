package com.tesora.dve.sql.node;

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

import java.util.Collection;

import com.tesora.dve.sql.util.ListSet;

public abstract class GeneralCollectingTraversal extends Traversal {

	protected ListSet<LanguageNode> collector;
	
	public GeneralCollectingTraversal(Order order, ExecStyle repeat) {
		super(order,repeat);
		collector = new ListSet<LanguageNode>();
	}
	
	public ListSet<LanguageNode> getCollected() {
		return collector;
	}
	
	public abstract boolean is(LanguageNode ln);
	
	@Override
	public final LanguageNode action(LanguageNode in) {
		if (is(in))
			collector.add(in);
		return in;
	}

	@SuppressWarnings("unchecked")
	public static <T extends LanguageNode> ListSet<T> cast(ListSet<LanguageNode> in) {
		ListSet<T> out = new ListSet<T>();
		for(LanguageNode ln : in)
			out.add((T)ln);
		return out;
	}

	public static <T extends LanguageNode> ListSet<T> collect(LanguageNode in, GeneralCollectingTraversal traversal) {
		traversal.traverse(in);
		return cast(traversal.getCollected());
	}
	
	public static <T extends LanguageNode> ListSet<T> collect(Edge<?,?> in, GeneralCollectingTraversal traversal) {
		traversal.traverse(in);
		return cast(traversal.getCollected());
	}

	public static <T extends LanguageNode> ListSet<T> collectEdges(Collection<Edge<?,?>> in, GeneralCollectingTraversal traversal) {
		traversal.traverseEdges(in);
		return cast(traversal.getCollected());
	}
	
	public static <T extends LanguageNode> ListSet<T> collect(Collection<? extends LanguageNode> in, GeneralCollectingTraversal traversal) {
		traversal.traverse(in);
		return cast(traversal.getCollected());
	}
	
}
