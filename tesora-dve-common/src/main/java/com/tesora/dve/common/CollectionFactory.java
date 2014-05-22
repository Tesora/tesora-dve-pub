package com.tesora.dve.common;

import java.util.Collection;

public interface CollectionFactory<T> {
	public Collection<T> create();

	public Collection<T> copy(final Collection<T> other);
}
