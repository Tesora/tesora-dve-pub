// OS_STATUS: public
package com.tesora.dve.sql.util;

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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

// essentially a LinkedHashSet with List operations
public class ListSet<T>  extends AbstractCollection<T> implements Set<T>, List<T> {

	private Set<T> set;
	private List<T> list;
	
	public ListSet() {
		this(new HashSet<T>(), new ArrayList<T>());
	}
	
	public ListSet(final int initialCapacity) {
		this(new HashSet<T>(initialCapacity), new ArrayList<T>(initialCapacity));
	}

	public ListSet(Set<T> backingSet, List<T> backingList) {
		set = backingSet;
		list = backingList;
	}
	
	public ListSet(Collection<T> in) {
		this();
		addAll(in);
	}
	
	public ListSet(T single) {
		this();
		add(single);
	}
	
	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		throw new UnsupportedOperationException("No support for addAll(int, Collection) in SetList");
	}

	@Override
	public T get(int index) {
		return list.get(index);
	}

	@Override
	public T set(int index, T element) {
		T was = null;
		if (set.add(element)) {
			was = list.set(index, element);
		} else {
			list.remove(element);
			was = list.set(index, element);
		}
		return was;
	}

	@Override
	public void add(int index, T element) {
		if (set.add(element)) {
			list.add(index, element);
		}
	}

	@Override
	public T remove(int index) {
		T obj = list.remove(index);
		set.remove(obj);
		return obj;
	}

	@Override
	public int indexOf(Object o) {
		return list.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o) {
		return list.indexOf(o);
	}

	@Override
	public ListIterator<T> listIterator() {
		throw new UnsupportedOperationException("No support for listIterator on ListSet");
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		throw new UnsupportedOperationException("No support for listIterator on ListSet");
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		return list.subList(fromIndex, toIndex);
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public boolean isEmpty() {
		return set.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return set.contains(o);
	}

	@Override
	public Object[] toArray() {
		return list.toArray();
	}

	@Override
	public <R> R[] toArray(R[] a) {
		return list.toArray(a);
	}

	@Override
	public boolean add(T e) {
		if (set.add(e)) {
			list.add(e);
			return true;
		}
		return false;
	}

	@Override
	public boolean remove(Object o) {
		if (set.remove(o)) {
			list.remove(o);
			return true;
		}
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return set.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		boolean changed = false;
		for(T t : c) {
			changed |= add(t);
		}
		return changed;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean changed = false;
		for(Iterator<T> iter = iterator(); iter.hasNext();) {
			T obj = iter.next();
			if (!c.contains(obj)) {
				changed = true;
				iter.remove();
			}
		}
		return changed;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean changed = false;
		for(Object o : c) 
			changed |= remove(o);
		return changed;
	}

	@Override
	public void clear() {
		set.clear();
		list.clear();
	}

	@Override
	public Iterator<T> iterator() {
		return new ListSetIterator<T>(list.iterator(), set);
	}

	private class ListSetIterator<L> implements Iterator<L> {

		L cachedCurrent;
		Iterator<L> backingIterator;
		Set<L> backingSet;
		
		public ListSetIterator(Iterator<L> actual, Set<L> bs) {
			backingIterator = actual;
			backingSet = bs;
			cachedCurrent = null;
		}
		
		@Override
		public boolean hasNext() {
			return backingIterator.hasNext();
		}

		@Override
		public L next() {
			cachedCurrent = backingIterator.next();
			return cachedCurrent;
		}

		@Override
		public void remove() {
			backingIterator.remove();
			backingSet.remove(cachedCurrent);
			cachedCurrent = null;
		}
		
	}

}
