package com.tesora.dve.common;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

public final class TreeSetFactory<V> implements CollectionFactory<V> {
	@Override
	public Set<V> create() {
		return new TreeSet<V>();
	}

	@Override
	public Set<V> copy(final Collection<V> other) {
		return new TreeSet<V>(other);
	}
}