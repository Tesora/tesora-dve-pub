package com.tesora.dve.common;

import java.util.Map;
import java.util.TreeMap;

public final class TreeMapFactory<K, V> implements MapFactory<K, V> {

	@Override
	public Map<K, V> create() {
		return new TreeMap<K, V>();
	}

	@Override
	public Map<K, V> copy(final Map<K, V> other) {
		return new TreeMap<K, V>(other);
	}

}
