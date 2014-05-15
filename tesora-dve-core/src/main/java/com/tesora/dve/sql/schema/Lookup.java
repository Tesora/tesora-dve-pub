// OS_STATUS: public
package com.tesora.dve.sql.schema;


import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.tesora.dve.sql.util.UnaryFunction;

public class Lookup<T extends Object> extends AbstractLookup<T> {
	protected Map<Name, T> objects = new LinkedHashMap<Name, T>();
	protected Map<Name, T> capObjects = new LinkedHashMap<Name, T>();

	public Lookup(boolean exactMatch, boolean isCaseSensitive, UnaryFunction<Name[], T> mapBy) {
		super(exactMatch,isCaseSensitive,mapBy);
	}
	
	public Lookup(Collection<T> objs, UnaryFunction<Name[], T> mapBy, boolean exactMatch, boolean isCaseSensitive) {
		this(exactMatch, isCaseSensitive, mapBy);
		refreshBacking(objs);
	}
	
	public Lookup<T> adapt(Collection<T> objs, UnaryFunction<Name[], T> mapBy) {
		return new Lookup<T>(objs, mapBy == null ? mapper : mapBy, exact, caseSensitive);
	}
	
	public Lookup<T> adapt(Collection<T> objs) {
		return adapt(objs, null);
	}
	
	@Override
	protected void allocateBacking(boolean cap, boolean append) {
		if (cap) {
			if(!append || capObjects == null) capObjects = new LinkedHashMap<Name,T>();
		} else {
			if (!append || objects == null) objects = new LinkedHashMap<Name,T>();
		}
	}
		
	@Override
	protected void store(boolean cap, Name n, T o) {
		if (!cap)
			objects.put(n,o);
		else
			capObjects.put(n, o);
	}
	
	@Override
	protected T get(boolean cap, Name n) {
		if (cap) {
			return capObjects.get(n);
		} else {
			return objects.get(n);
		}
	}
	
}
