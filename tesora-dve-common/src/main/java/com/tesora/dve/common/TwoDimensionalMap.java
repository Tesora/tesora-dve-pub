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


import java.util.Map;
import java.util.Set;

public final class TwoDimensionalMap<OuterKey, InnerKey, Value> {

	private Map<OuterKey, Map<InnerKey, Value>> backing;
	private final MapFactory<InnerKey, Value> innerMapFactory;
	
	public TwoDimensionalMap() {
		this(new HashMapFactory<OuterKey, Map<InnerKey, Value>>(), new HashMapFactory<InnerKey, Value>());
	}
	
	public TwoDimensionalMap(final MapFactory<OuterKey, Map<InnerKey, Value>> outerMapFactory, final MapFactory<InnerKey, Value> innerMapFactory) {
		this.backing = outerMapFactory.create();
		this.innerMapFactory = innerMapFactory;
	}
	
	public Value put(final OuterKey ok, final InnerKey ik, final Value nv) {
		Map<InnerKey, Value> inner = get(ok);
		if (inner == null) {
			inner = innerMapFactory.create();
			backing.put(ok, inner);
		}

		return inner.put(ik, nv);
	}
	
	public Map<InnerKey, Value> get(final OuterKey ok) {
		return backing.get(ok);
	}
	
	public Value get(final OuterKey ok, final InnerKey ik) {
		final Map<InnerKey, Value> inner = get(ok);
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
