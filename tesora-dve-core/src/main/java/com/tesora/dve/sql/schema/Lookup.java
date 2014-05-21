// OS_STATUS: public
package com.tesora.dve.sql.schema;

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
