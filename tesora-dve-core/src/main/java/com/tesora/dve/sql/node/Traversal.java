// OS_STATUS: public
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
import java.util.List;

public abstract class Traversal extends AbstractTraversal<LanguageNode> {
	
	public Traversal(Order direction, ExecStyle execStyle) {
		super(direction, execStyle);
	}

	public boolean allow(Edge<?,?> e) {
		return true;
	}
		
	private <T extends Edge<?,?>> List<T> getEdges(LanguageNode in) {
		if (order == Order.NATURAL_ORDER)
			return in.getNaturalOrderEdges();
		return in.getEdges();
	}
	
	protected void traverseInternal(LanguageNode n) {
		for(Edge<? extends LanguageNode, ? extends LanguageNode> e : getEdges(n)) {
			if (allow(e))
				traverse(e);
		}
	}

	public final void traverse(Collection<? extends LanguageNode> n) {
		for(LanguageNode ln : n)
			traverse(ln);
	}
	
	public final void traverseEdges(Collection<Edge<?,?>> in) {
		for(Edge<?,?> e : in)
			if (allow(e))
				traverse(e);
	}
	
	public final void traverse(Edge<?,?> e) {
		if (e == null) return;
		if (e.isMultiMulti()) {
			MultiMultiEdge<?, ? extends LanguageNode> me = (MultiMultiEdge<?, ? extends LanguageNode>)e;
			for(MultiEdge<?, ? extends LanguageNode> sube : me.getMultiEdges()) {
				traverse(sube);
			}
		} else if (e.isMulti()) {
			MultiEdge<?, ? extends LanguageNode> se = (MultiEdge<?,? extends LanguageNode>) e;
			for(SubEdge<?,? extends LanguageNode> sube : se.getEdges()) {
				traverse(sube.get());
			}
		} else {
			SingleEdge<?,? extends LanguageNode> se = (SingleEdge<?,? extends LanguageNode>) e;
			traverse(se.get());
		}
	}
		
	protected final LanguageNode performAction(LanguageNode in) {
		Edge<?,LanguageNode> owner = (Edge<?, LanguageNode>) in.getParentEdge();
		LanguageNode out = action(in);
		if (in != out && owner != null)
			owner.set(out);
		return out;
	}
	

}
