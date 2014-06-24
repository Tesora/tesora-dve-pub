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

public final class TwoDimensionalMultiMap<OuterKey, InnerKey, Value> {

	private Map<OuterKey, MultiMap<InnerKey, Value>> backing;
	private final MapFactory<InnerKey, Collection<Value>> innerMapFactory;
	private final CollectionFactory<Value> valueStorageFactory;
	
	public TwoDimensionalMultiMap() {
		this(new HashMapFactory<OuterKey, MultiMap<InnerKey, Value>>(),
				new HashMapFactory<InnerKey, Collection<Value>>(),
				new ArrayListFactory<Value>());
	}
	
	public TwoDimensionalMultiMap(
			final MapFactory<OuterKey, MultiMap<InnerKey, Value>> outerMapFactory,
			final MapFactory<InnerKey, Collection<Value>> innerMapFactory,
			final CollectionFactory<Value> valueStorageFactory) {
		this.backing = outerMapFactory.create();
		this.innerMapFactory = innerMapFactory;
		this.valueStorageFactory = valueStorageFactory;
	}
	
	public boolean put(final OuterKey ok, final InnerKey ik, final Value value) {
		final MultiMap<InnerKey, Value> inner = locateInternalValueStorage(ok);
		return inner.put(ik, value);
	}
	
	public boolean putAll(final OuterKey ok, final InnerKey ik, final Collection<Value> values) {
		final MultiMap<InnerKey, Value> inner = locateInternalValueStorage(ok);
		return inner.putAll(ik, values);
	}

	public void putAll(final TwoDimensionalMultiMap<OuterKey, InnerKey, Value> other) {
		for (final OuterKey ok : other.keySet()) {
			final MultiMap<InnerKey, Value> inner = locateInternalValueStorage(ok);
			inner.putAll(other.get(ok));
		}
	}

	private MultiMap<InnerKey, Value> locateInternalValueStorage(final OuterKey ok) {
		MultiMap<InnerKey, Value> inner = get(ok);
		if (inner == null) {
			inner = new MultiMap<InnerKey, Value>(innerMapFactory, valueStorageFactory);
			backing.put(ok, inner);
		}

		return inner;
	}

	public MultiMap<InnerKey, Value> get(final OuterKey ok) {
		return backing.get(ok);
	}
	
	public Collection<Value> get(final OuterKey ok, final InnerKey ik) {
		final MultiMap<InnerKey, Value> inner = get(ok);
		if (inner != null) {
			return inner.get(ik);
		}

		return null;
	}
	
	public Set<OuterKey> keySet() {
		return backing.keySet();
	}
	
	public void clear() {
		backing.clear();
	}
}
