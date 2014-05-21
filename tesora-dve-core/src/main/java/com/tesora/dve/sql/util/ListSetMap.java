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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ListSetMap<K, V> extends AbstractMap<K, V> {

	private final ListSet<Entry<K, V>> storage;

	public ListSetMap() {
		this.storage = new ListSet<Entry<K, V>>();
	}

	public ListSetMap(final int initialCapacity) {
		this.storage = new ListSet<Entry<K, V>>(initialCapacity);
	}

	@Override
	public void clear() {
		storage.clear();
	}

	@Override
	protected ListSetMap<K, V> clone() throws CloneNotSupportedException {
		final ListSetMap<K, V> clone = new ListSetMap<K, V>(this.size());
		clone.storage.addAll(this.storage);
		return clone;
	}

	@Override
	public boolean containsKey(Object key) {
		return this.getKeyIndex(key) > -1;
	}

	@Override
	public boolean containsValue(Object value) {
		return this.getValueIndex(value) > -1;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return Collections.unmodifiableSet(this.storage);
	}

	@Override
	public boolean equals(Object other) {
		if (other == this) {
			return true;
		}

		if (other instanceof ListSetMap<?, ?>) {
			final ListSetMap<?, ?> otherMap = (ListSetMap<?, ?>) other;
			return this.storage.equals(otherMap.storage);
		}

		return false;
	}

	@Override
	public V get(Object key) {
		final int keyIndex = getKeyIndex(key);
		if (keyIndex > -1) {
			final Entry<K, V> e = this.storage.get(keyIndex);
			return e.getValue();
		}

		return null;
	}

	@Override
	public int hashCode() {
		return this.storage.hashCode();
	}

	@Override
	public boolean isEmpty() {
		return this.storage.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		final Set<K> keySet = new HashSet<K>(this.size());
		for (final Entry<K, V> e : this.storage) {
			keySet.add(e.getKey());
		}

		return Collections.unmodifiableSet(keySet);
	}

	@Override
	public V put(K key, V value) {
		if (key == null) {
			throw new NullPointerException("This map does not permit null keys.");
		}

		final int keyIndex = this.getKeyIndex(key);
		if (keyIndex > -1) {
			return this.storage.get(keyIndex).setValue(value);
		}

		this.storage.add(new SimpleEntry<K, V>(key, value));

		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (final Entry<? extends K, ? extends V> e : m.entrySet()) {
			this.put(e.getKey(), e.getValue());
		}
	}

	@Override
	public V remove(Object key) {
		final int keyIndex = this.getKeyIndex(key);
		if (keyIndex > -1) {
			final Entry<K, V> e = this.storage.get(keyIndex);
			this.storage.remove(keyIndex);
			return e.getValue();
		}

		return null;
	}

	@Override
	public int size() {
		return this.storage.size();
	}

	@Override
	public String toString() {
		return this.storage.toString();
	}

	@Override
	public Collection<V> values() {
		final List<V> values = new ArrayList<V>(this.size());
		for (final Entry<K, V> e : this.storage) {
			values.add(e.getValue());
		}

		return Collections.unmodifiableCollection(values);
	}

	/**
	 * Put a new entry at a given position in the map.
	 * If the key already exists and is not in place,
	 * move it to the specified index.
	 * 
	 * Return the previous value associated with key,
	 * or null if there was no mapping for key.
	 */
	public V put(final int index, final K key, final V value) {
		if ((index < 0) || (index >= this.size())) {
			throw new IllegalArgumentException("Index <" + index + "> out of bounds.");
		}

		if (key == null) {
			throw new NullPointerException("This map does not permit null keys.");
		}

		final int keyIndex = this.getKeyIndex(key);
		if (keyIndex > -1) {
			final Entry<K, V> e = this.storage.get(keyIndex);

			/* Move the entry only if not already in place. */
			if (keyIndex != index) {
				this.storage.remove(e);
				this.storage.add(index, e);
			}

			/* Assign the new value. */
			return e.setValue(value);
		}

		/* Insert a new one if it does not exist. */
		this.storage.add(index, new SimpleEntry<K, V>(key, value));

		return null;
	}

	/**
	 * Swap two entries within the map.
	 */
	public void swap(final K key1, final K key2) {
		if ((key1 == null) || (key2 == null)) {
			throw new NullPointerException("This map does not permit null keys.");
		}

		final int keyIndex1 = this.getKeyIndex(key1);
		final int keyIndex2 = this.getKeyIndex(key2);

		if ((keyIndex1 < 0) || (keyIndex2 < 0)) {
			throw new IllegalArgumentException("Key not found.");
		}

		final Entry<K, V> e1 = this.storage.get(keyIndex1);
		final Entry<K, V> e2 = this.storage.get(keyIndex2);
		this.storage.set(keyIndex1, null);
		this.storage.set(keyIndex2, e1);
		this.storage.set(keyIndex1, e2);
	}

	public Entry<K, V> getEntryAt(final int index) {
		if ((index < 0) || (index >= this.size())) {
			throw new IllegalArgumentException("Index <" + index + "> out of bounds.");
		}

		return this.storage.get(index);
	}

	public List<Entry<K, V>> entryList() {
		return Collections.unmodifiableList(this.storage);
	}

	protected int getKeyIndex(final Object key) {
		int index = 0;
		for (final Entry<K, V> e : this.storage) {
			if (e.getKey().equals(key)) {
				return index;
			}
			++index;
		}

		return -1;
	}

	protected int getValueIndex(final Object value) {
		int index = 0;
		for (final Entry<K, V> e : this.storage) {
			final Object v = e.getValue();
			if ((v != null) && v.equals(value)) {
				return index;
			}
			++index;
		}

		return -1;
	}

}
