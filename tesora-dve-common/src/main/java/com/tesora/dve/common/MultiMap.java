// OS_STATUS: public
package com.tesora.dve.common;


import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class MultiMap<K, V> {

	private final Map<K, Collection<V>> backing;
	private final CollectionFactory<V> factory; 
	
	public MultiMap(MapFactory<K, V> backingFactory, CollectionFactory<V> subFactory) {
		backing = backingFactory.create();
		factory = subFactory;
	}
	
	public MultiMap() {
		this(new MapFactory<K, V>(), new CollectionFactory<V>());
	}
	
	public MultiMap(CollectionFactory<V> cf) {
		this(new MapFactory<K, V>(), cf);
	}
	
	public MultiMap(MapFactory<K, V> backingFactory) {
		this(backingFactory, new CollectionFactory<V>());
	}
	
	public boolean isEmpty() {
		return backing.isEmpty();
	}

	public boolean containsKey(K key) {
		return backing.containsKey(key);
	}

	public Collection<V> get(K key) {
		return backing.get(key);
	}
	
	public boolean put(K key, V value) {
		Collection<V> sub = backing.get(key);
		if (sub == null) {
			sub = factory.create();
			backing.put(key, sub);
		}
		return sub.add(value);
	}
	
	public <C extends Collection<V>> boolean put(K key, C values) {
		Collection<V> sub = backing.get(key);
		if (sub == null) {
			sub = factory.create();
			backing.put(key, sub);
		}
		return sub.addAll(values);
	}
	
	public boolean contains(K key, V value) {
		Collection<V> sub = get(key);
		if (sub == null || sub.isEmpty())
			return false;
		return sub.contains(value);
	}
	
	public boolean remove(K key, V value) {
		Collection<V> sub = get(key);
		if (sub == null)
			return false;
		else if (sub.isEmpty()) {
			backing.remove(key);
			return false;
		} 
		boolean retval = sub.remove(value);
		if (sub.isEmpty()) {
			backing.remove(key);
			return true;
		}
		return retval;
	}

	public boolean remove(K key) {
		Collection<V> sub = get(key);
		if (sub == null)
			return false;
		else if (sub.isEmpty()) {
			backing.remove(key);
			return false;
		}
		backing.remove(key);
		return true;
	}
	
	public void clear() {
		backing.clear();
	}

	public Set<K> keySet() {
		return backing.keySet();
	}

	public Collection<V> values() {
		Collection<V> buf = factory.create();
		for(Collection<V> v : backing.values()) 
			buf.addAll(v);
		return buf;
	}
	
	public static class MapFactory<K, V> {
		
		public Map<K, Collection<V>> create() {
			return new LinkedHashMap<K, Collection<V>>();
		}
	}

	public static class OrderedMapFactory<K, V> extends MapFactory<K, V> {
		@Override
		public Map<K, Collection<V>> create() {
			return new TreeMap<K, Collection<V>>();
		}
	}
	
	public static class CollectionFactory<V> {
		public Collection<V> create() {
			return new ArrayList<V>();
		}
	}

	public static class HashedCollectionFactory<V> extends CollectionFactory<V> {
		@Override
		public Collection<V> create() {
			return new LinkedHashSet<V>();
		}
	}	
}
