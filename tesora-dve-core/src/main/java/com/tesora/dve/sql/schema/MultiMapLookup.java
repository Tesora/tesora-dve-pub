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

import java.util.ArrayList;
import java.util.Collection;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.util.UnaryFunction;

public class MultiMapLookup<T extends Object> extends AbstractLookup<Collection<T>>{

	public MultiMapLookup(boolean exactMatch, boolean isCaseSensitive, final UnaryFunction<Name[], T> mapBy) {
		super(exactMatch, isCaseSensitive, new UnaryFunction<Name[], Collection<T>>() {

			@Override
			public Name[] evaluate(Collection<T> object) {
				ArrayList<Name> out = new ArrayList<Name>();
				for(T o : object) {
					Name[] ns = mapBy.evaluate(o);
					for(Name n : ns)
						out.add(n);
				}
				return (Name[])out.toArray(new Name[0]);
			}
			
		});
	}
	
	protected MultiMap<Name, T> objects = new MultiMap<Name, T>();
	protected MultiMap<Name, T> capObjects = new MultiMap<Name, T>();
	
	@Override
	protected void store(boolean cap, Name n, Collection<T> o) {
		if (cap) {
			capObjects.putAll(n, o);
		} else {
			objects.putAll(n, o);
		}
	}
	
	@Override
	protected Collection<T> get(boolean cap, Name n) {
		if (cap) {
			return capObjects.get(n);
		} else {
			return objects.get(n);
		}
	}

	@Override
	protected void allocateBacking(boolean cap, boolean append) {
		if (cap) {
			if(!append || capObjects == null) capObjects = new MultiMap<Name,T>();
		} else {
			if (!append || objects == null) objects = new MultiMap<Name,T>();
		}
	}

	
	
}
