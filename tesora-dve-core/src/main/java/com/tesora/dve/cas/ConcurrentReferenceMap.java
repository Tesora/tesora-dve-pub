// OS_STATUS: public
package com.tesora.dve.cas;

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
