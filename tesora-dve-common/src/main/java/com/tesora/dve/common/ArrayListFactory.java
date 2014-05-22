package com.tesora.dve.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class ArrayListFactory<V> implements CollectionFactory<V> {
	@Override
	public List<V> create() {
		return new ArrayList<V>();
	}

	@Override
	public List<V> copy(final Collection<V> other) {
		return new ArrayList<V>(other);
	}
}