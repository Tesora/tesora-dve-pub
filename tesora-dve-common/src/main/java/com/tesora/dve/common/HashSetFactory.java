package com.tesora.dve.common;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public final class HashSetFactory<V> implements CollectionFactory<V> {
	@Override
	public Set<V> create() {
		return new HashSet<V>();
	}

	@Override
	public Set<V> copy(final Collection<V> other) {
		return new HashSet<V>(other);
	}
}