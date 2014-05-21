// OS_STATUS: public
package com.tesora.dve.cas.impl;

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

import com.tesora.dve.cas.AtomicState;
import com.tesora.dve.cas.ConcurrentReferenceMap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


//A special purpose concurrent map that uses identity comparisions rather than equals(), so that concurrent map calls like putIfAbsent() and replace() behave more like AtomicReference.compareAndSet().
public class ConcurrentReferenceMapImpl<K,V> implements ConcurrentReferenceMap<K,V> {

    ConcurrentMap<K,IdentityValue<V>> backingMap = new ConcurrentHashMap<K, IdentityValue<V>>();


    @Override
    public AtomicState<V> binding(K key){
        return new BoundAtomicState<K,V>(this,key);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        IdentityValue<V> wrappedValue = new IdentityValue<V>(value);
        IdentityValue<V> existing = backingMap.putIfAbsent(key,wrappedValue);
        if (existing == null)
            return null;
        else
            return existing.value;
    }

    @Override
    public boolean remove(Object key, Object value) {
        IdentityValue<Object> wrappedValue = new IdentityValue<Object>(value);
        return backingMap.remove(key,wrappedValue);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        IdentityValue<V> expectedValue = new IdentityValue<V>(oldValue);
        IdentityValue<V> wrappedValue = new IdentityValue<V>(newValue);
        return backingMap.replace(key,expectedValue,wrappedValue);
    }

    @Override
    public V replace(K key, V value) {
        IdentityValue<V> wrappedValue = new IdentityValue<V>(value);
        IdentityValue<V> existing = backingMap.replace(key,wrappedValue);
        if (existing == null)
            return null;
        else
            return existing.value;
    }

    @Override
    public int size() {
        return backingMap.size();
    }

    @Override
    public boolean isEmpty() {
        return backingMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return backingMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        IdentityValue<Object> wrappedValue = new IdentityValue<Object>(value);
        return backingMap.containsValue(wrappedValue);
    }

    @Override
    public V get(Object key) {
        IdentityValue<V> existing = backingMap.get(key);
        if (existing == null)
            return null;
        else
            return existing.value;
    }

    @Override
    public V put(K key, V value) {
        IdentityValue<V> wrappedValue = new IdentityValue<V>(value);
        IdentityValue<V> existing = backingMap.put(key,wrappedValue);
        if (existing == null)
            return null;
        else
            return existing.value;
    }

    @Override
    public V remove(Object key) {
        IdentityValue<V> existing = backingMap.remove(key);
        if (existing == null)
            return null;
        else
            return existing.value;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
            this.put(e.getKey(), e.getValue());
    }

    @Override
    public void clear() {
        backingMap.clear();
    }

    @Override
    public Set<K> keySet() {
        return backingMap.keySet();
    }

    @Override
    public Collection<V> values() {
        ArrayList<V> unwrapped = new ArrayList<V>();
        Collection<IdentityValue<V>> wrappedValues = backingMap.values();
        for (IdentityValue<V> entry : wrappedValues)
            unwrapped.add(entry.value);
        return unwrapped;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        HashSet< Entry<K,V> > entries = new HashSet<Entry<K, V>>();

        for (Map.Entry<K,IdentityValue<V>> entry : backingMap.entrySet())
            entries.add( new AbstractMap.SimpleImmutableEntry<K,V>(entry.getKey(),entry.getValue().value));
        return entries;
    }


    static class IdentityValue<X> {
        X value;

        IdentityValue(X value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof IdentityValue))
                return false;
            IdentityValue otherValue = (IdentityValue)other;
            return this.value == otherValue.value;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.value);
        }
    }

    static class BoundAtomicState<X,Y> implements AtomicState<Y> {
        ConcurrentReferenceMapImpl<X,Y> backingMap;
        X boundKey;

        BoundAtomicState(ConcurrentReferenceMapImpl<X, Y> backingMap, X boundKey) {
            this.backingMap = backingMap;
            this.boundKey = boundKey;
        }

        @Override
        public Y get() {
            return this.backingMap.get(boundKey);
        }

        @Override
        public boolean compareAndSet(Y expected, Y value) {
            if (expected == null){
                Y existing = backingMap.putIfAbsent(boundKey,value);
                return (existing == null);
            } else {
                return backingMap.replace(boundKey,expected,value);
            }
        }
    }
}
