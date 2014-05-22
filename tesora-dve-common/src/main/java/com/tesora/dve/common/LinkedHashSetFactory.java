package com.tesora.dve.common;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public final class LinkedHashSetFactory<V> implements CollectionFactory<V> {
	@Override
	public Set<V> create() {
		return new LinkedHashSet<V>();
	}

	@Override
	public Set<V> copy(final Collection<V> other) {
		return new LinkedHashSet<V>(other);
	}
}