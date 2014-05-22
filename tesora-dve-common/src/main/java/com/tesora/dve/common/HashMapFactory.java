package com.tesora.dve.common;

import java.util.HashMap;
import java.util.Map;

public final class HashMapFactory<K, V> implements MapFactory<K, V> {
	@Override
	public Map<K, V> create() {
		return new HashMap<K, V>();
	}

	@Override
	public Map<K, V> copy(final Map<K, V> other) {
		return new HashMap<K, V>(other);
	}
}
