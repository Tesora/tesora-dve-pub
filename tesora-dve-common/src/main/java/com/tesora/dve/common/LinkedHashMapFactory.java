package com.tesora.dve.common;

import java.util.LinkedHashMap;
import java.util.Map;

public final class LinkedHashMapFactory<K, V> implements MapFactory<K, V> {
	@Override
	public Map<K, V> create() {
		return new LinkedHashMap<K, V>();
	}

	@Override
	public Map<K, V> copy(final Map<K, V> other) {
		return new LinkedHashMap<K, V>(other);
	}
}
