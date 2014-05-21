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

import java.util.List;

public abstract class Edge<O extends LanguageNode, T extends LanguageNode> {

	protected O parent;
	protected Class<O> parentClass;
	protected IEdgeName name;
	
	public Edge(Class<O> oc, O owner, IEdgeName edgeName) {
		parentClass = oc;
		parent = owner;
		name = edgeName;
	}
	
	public O getParent() {
		return parent;
	}
	
	public Class<O> getParentClass() {
		return parentClass;
	}
	
	public IEdgeName getName() {
		return name;
	}
	
	public abstract boolean has();
	public abstract boolean isMulti();
	public abstract boolean isMultiMulti();
	
	public abstract T get();
	public abstract List<T> getMulti();
	
	public abstract void set(T in);
	public abstract void set(List<T> in);
	
	public abstract void add(T in);
	public abstract void add(List<T> in);
	
	public abstract void clear();
	
	public abstract boolean schemaEqual(Edge<O,T> other);
	
	public final int schemaHash() {
		return LanguageNode.addSchemaHash(getName().hashCode(), schemaTargetHash());
	}
	
	protected abstract int schemaTargetHash();
	
	// path support.  given an input language node, apply this edge to the input language node to get a new node
	public LanguageNode apply(LanguageNode in) {
		Edge<?,?> matching = find(in);
		return matching.get();
	}
	
	// find the edge matching this edge in the input node
	public Edge<?,?> find(LanguageNode in) {
		List<Edge<?,?>> edges = in.getEdges();
		for(Edge<?,?> e : edges) {
			if (e.getName().matches(this.getName())) 
				return e;
		}
		throw new MigrationException("No such edge name in " + in + ": " + this.getName());		
	}
	
	@Override
	public String toString() {
		return "Edge{" + parentClass.getName() + "." + name.toString() + "}";
	}
}
