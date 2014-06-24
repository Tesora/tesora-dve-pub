package com.tesora.dve.common;

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
import java.util.Map;
import java.util.Set;

public final class MultiMap<K, V> {

	private final Map<K, Collection<V>> backing;
	private final CollectionFactory<V> valueStorageFactory;

	public MultiMap(final MapFactory<K, Collection<V>> mapFactory, final CollectionFactory<V> valueStorageFactory) {
		this.backing = mapFactory.create();
		this.valueStorageFactory = valueStorageFactory;
	}
	
	public MultiMap(final MapFactory<K, Collection<V>> mapFactory) {
		this(mapFactory, new ArrayListFactory<V>());
	}

	public MultiMap(final CollectionFactory<V> valueStorageFactory) {
		this(new LinkedHashMapFactory<K, Collection<V>>(), valueStorageFactory);
	}
	
	public MultiMap() {
		this(new LinkedHashMapFactory<K, Collection<V>>(), new ArrayListFactory<V>());
	}
	
	public boolean isEmpty() {
		return backing.isEmpty();
	}

	public boolean containsKey(K key) {
		return backing.containsKey(key);
	}

	public Collection<V> get(final K key) {
		return backing.get(key);
	}
	
	public boolean put(final K key, final V value) {
		final Collection<V> valueStorage = locateInternalValueStorage(key);
		return valueStorage.add(value);
	}
	
	public boolean putAll(final K key, final Collection<V> values) {
		final Collection<V> valueStorage = locateInternalValueStorage(key);
		return valueStorage.addAll(values);
	}
	
	public void putAll(final MultiMap<K, V> other) {
		for (final K key : other.keySet()) {
			final Collection<V> valueStorage = locateInternalValueStorage(key);
			valueStorage.addAll(other.get(key));
		}
	}

	private Collection<V> locateInternalValueStorage(final K key) {
		Collection<V> valueStorage = backing.get(key);
		if (valueStorage == null) {
			valueStorage = valueStorageFactory.create();
			backing.put(key, valueStorage);
		}

		return valueStorage;
	}

	public boolean contains(final K key, final V value) {
		final Collection<V> valueStorage = get(key);
		if ((valueStorage != null) && (!valueStorage.isEmpty())) {
			return valueStorage.contains(value);
		}

		return false;
	}
	
	public boolean remove(final K key, final V value) {
		final Collection<V> valueStorage = get(key);
		if (valueStorage == null) {
			return false;
		} else if (valueStorage.isEmpty()) {
			backing.remove(key);
			return false;
		}

		final boolean removed = valueStorage.remove(value);
		if (valueStorage.isEmpty()) {
			backing.remove(key);
			return true;
		}

		return removed;
	}

	public boolean remove(final K key) {
		final Collection<V> valueStorage = get(key);
		if (valueStorage == null) {
			return false;
		} else if (valueStorage.isEmpty()) {
			backing.remove(key);
			return false;
		}

		backing.remove(key);

		return true;
	}
	
	public void clear() {
		backing.clear();
	}

	public Set<K> keySet() {
		return backing.keySet();
	}

	public Collection<V> values() {
		final Collection<V> values = valueStorageFactory.create();
		for (final Collection<V> valueStorage : backing.values()) {
			values.addAll(valueStorage);
		}

		return values;
	}
}
