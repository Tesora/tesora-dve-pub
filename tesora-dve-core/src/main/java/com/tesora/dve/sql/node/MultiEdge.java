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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class MultiEdge<O extends LanguageNode, T extends LanguageNode> extends Edge<O,T> implements Iterable<T> {

	protected List<SubEdge<O,T>> target;
	
	public MultiEdge(Class<O> oc, O owner, EdgeName name) {
		super(oc, owner, name);
		target = new ArrayList<SubEdge<O,T>>();
	}

	@Override
	public boolean has() {
		return !target.isEmpty();
	}

	@Override
	public boolean isMulti() {
		return true;
	}
	
	@Override
	public boolean isMultiMulti() {
		return false;
	}
	
	public int size() {
		return target.size();
	}
	
	public T get(int i) {
		if (target.isEmpty()) return null;
		return target.get(i).get();
	}

	public void remove(int i) {
		target.remove(i);
	}
	
	public T getLast() {
		if (target.isEmpty()) return null;
		return target.get(target.size() - 1).get();
	}
	
	public void removeLast() {
		if (target.isEmpty()) return;
		target.remove(target.size() - 1);
	}
	
	public void addAll(Collection<T> in) {
		for(T t : in)
			add(t);
	}

	

	public SubEdge<O, T> getEdge(int i) {
		return target.get(i);
	}
	
	public boolean isEmpty() {
		return target.isEmpty();
	}
	
	@Override
	public T get() {
		if (target.isEmpty()) return null;
		return target.get(0).get();
	}

	@Override
	public List<T> getMulti() {
		return Collections.unmodifiableList(
		Functional.apply(target, new UnaryFunction<T, SubEdge<O,T>>() {

			@Override
			public T evaluate(SubEdge<O, T> object) {
				return object.get();
			}
			
		}));
	}

	@Override
	public void set(T in) {
		set(Collections.singletonList(in));
	}

	@Override
	public void set(List<T> in) {
		for(SubEdge<O, T> se : target)
			se.clear();
		target.clear();
		if (in != null) {
			for(T t : in)
				target.add(makeSubEdge(t,target.size()));
		}
	}

	public List<SubEdge<O,T>> getEdges() {
		return target;
	}
	
	@Override
	public Iterator<T> iterator() {
		return new MultiEdgeIterator<O,T>(this);
	}
	
	private SubEdge<O,T> makeSubEdge(T o, int offset) {
		SubEdge<O,T> ose = new SubEdge<O, T>(getParentClass(),getParent(),getName().makeOffset(offset));
		ose.set(o);
		return ose;
	}
	
	@Override
	public boolean schemaEqual(Edge<O,T> other) {
		if (other == null) return false;
		if (!(other instanceof MultiEdge)) return false;
		MultiEdge<O,T> ome = (MultiEdge<O,T>) other;
		if (target.size() != ome.target.size()) return false;
		for(int i = 0; i < target.size(); i++) {
			if (!target.get(i).schemaEqual(ome.target.get(i)))
				return false;
		}
		return true;
	}

	@Override
	protected int schemaTargetHash() {
		int result = 0;
		for(int i = 0; i < target.size(); i++) 
			result = LanguageNode.addSchemaHash(result, target.get(i).schemaHash());
		return result;
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public void clear() {
		set(Collections.EMPTY_LIST);
	}

	@Override
	public LanguageNode apply(LanguageNode in) {
		// should never get these, so this is an error
		throw new MigrationException("Unmigrated code path: MultiEdge.apply");
	}
	
	protected static class MultiEdgeIterator<O extends LanguageNode, T extends LanguageNode> implements Iterator<T> {

		protected MultiEdge<O,T> owner;
		protected int position;
		
		protected MultiEdgeIterator(MultiEdge<O,T> o) {
			owner = o;
			position = -1;
		}
		
		@Override
		public boolean hasNext() {
			return (position + 1) < owner.getEdges().size();
		}

		@Override
		public T next() {
			position++;
			return owner.getEdge(position).get();
		}

		@Override
		public void remove() {
			owner.remove(position);
			position--;
		}
		
	}

	@Override
	public void add(List<T> in) {
		for(T t: in)
			add(t);
	}

	@Override
	public void add(T value) {
		target.add(makeSubEdge(value,target.size()));
	}


	
}
