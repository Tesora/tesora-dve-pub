package com.tesora.dve.common;

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
