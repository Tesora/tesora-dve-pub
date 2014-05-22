package com.tesora.dve.cas;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.concurrent.ConcurrentMap;

/**
 * A special purpose concurrent map that uses identity comparisions on values, so that concurrent map calls
 * like putIfAbsent() and replace() behave more like AtomicReference.compareAndSet().
 * <br/>
 * Similar to IdentityHashMap, this does not honor the equals() contract laid out by Map, and is not suitable for general usage.
**/
public interface ConcurrentReferenceMap<K, V> extends ConcurrentMap<K,V> {
    AtomicState<V> binding(K key);
}
