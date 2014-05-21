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
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.CopyContext;

public abstract class LanguageNode {

	protected Edge<?,? extends LanguageNode> parent = null;

	private EngineBlock block = null;
	protected SourceLocation location;
	
	public LanguageNode(SourceLocation loc) {
		location = loc;
	}
	
	protected LanguageNode(LanguageNode ln) {
		location = ln.location;
	}
	
	public SourceLocation getSourceLocation() {
		return location;
	}
	
	public <T extends LanguageNode> void setParent(Edge<?,T> p) {
		this.parent = p;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends LanguageNode> Edge<?, T> getParentEdge() {
		return (Edge<?, T>) this.parent;
	}
		
	public LanguageNode getParent() {
		return (this.parent == null ? null : this.parent.getParent());
	}
	
	public EngineBlock getBlock() {
		if (block == null) block = new EngineBlock(this);
		return block;
	}
	
	public <T> T getDerivedAttribute(DerivedAttribute<T> da, SchemaContext sc) {
		return getBlock().getValue(da, sc);
	}
	
	// find the enclosing object of type searchFor, stopping either at null or an object of type stopAt
	@SuppressWarnings("unchecked")
	public <T> T getEnclosing(Class<T> searchFor, Class<?> stopAt) {
		LanguageNode p = this;
		while(p != null) {
			if (searchFor.isInstance(p))
				return (T)p;
			else if (stopAt != null && stopAt.isInstance(p))
				return null;
			if (p.getParent() != null)
				p = p.getParent();
			else
				p = null;
		}
		return null;
	}

	public abstract <T extends Edge<?,?>> List<T> getEdges();
	
	public <T extends Edge<?,?>> List<T> getNaturalOrderEdges() {
		return getEdges();
	}
	
	protected <T extends LanguageNode> void replaceChild(Edge<?,T> edge, T newChild) {
		edge.set(newChild);
	}

	public <T extends Edge<? extends LanguageNode,?>> T getAttribute(IEdgeName name) {
		List<T> edges = getEdges();
		for(T e : edges) {
			if (e.getName().matches(name))
				return e;
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends LanguageNode> T ifAncestor(Collection<T> any) {
		if (any.contains(this))
			return (T)this;
		else if (parent == null)
			return null;
		else if (parent.getParent() != null)
			return parent.getParent().ifAncestor(any);
		else
			return null;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private final boolean edgesEqual(LanguageNode other) {
		// we're equal if we have the same number of edges, we have all the
		// same edges, and the contents are equal.
		List<? extends Edge> mine = getEdges();
		List<? extends Edge> yours = other.getEdges();
		// because the edge order is fixed in each subclass - we can just iterate over each list
		// and as soon as we get a not matching name - we're done.
		if (mine.size() != yours.size())
			return false;
		Iterator<? extends Edge> imine = mine.iterator();
		Iterator<? extends Edge> iyours = yours.iterator();
		while(imine.hasNext() && iyours.hasNext()) {
			Edge me = imine.next();
			Edge ye = iyours.next();
			if (!me.getName().matches(ye.getName()))
				return false;
			if (!me.schemaEqual(ye))
				return false;
		}
		return true;
	}
	
	
	protected abstract boolean schemaSelfEqual(LanguageNode other);
	
	public final boolean isSchemaEqual(LanguageNode in) {
		if (in == this)
			return true;
		else if (this.getClass().equals(in.getClass())) {
			if (!schemaSelfEqual(in)) return false;
			return edgesEqual(in);
		} else
			return false;
	}

	private final int edgesHashCode() {
		int result = 0;
		for(Edge<?,?> e : getEdges()) {
			result = addSchemaHash(result,e.schemaHash());
		}
		return result;
	}
	
	public final int getSchemaHashCode() {
		return addSchemaHash(addSchemaHash(initSchemaHash(),selfHashCode()),edgesHashCode());
	}
	
	protected abstract int selfHashCode();
		
	protected int initSchemaHash() {
		return this.getClass().hashCode();
	}
	
	public static int addSchemaHash(int result, boolean v) {
		return addSchemaHash(result, (v ? 1231 : 1237));
	}
	
	public static int addSchemaHash(int result, int hc) {
		final int prime = 31;
		return prime * result + hc;
	}

	public Edge<?,?> getMatchingAncestor(EdgeName en) {
		if (parent == null)
			return null;
		else if (parent.getName().matches(en))
			return parent;
		else
			return parent.getParent().getMatchingAncestor(en);
	}

	public abstract LanguageNode copy(CopyContext in);

	
}
