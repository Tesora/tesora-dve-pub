package com.tesora.dve.common;

import java.util.Map;

public interface MapFactory<K, V> {
	public Map<K, V> create();

	public Map<K, V> copy(final Map<K, V> other);
}
