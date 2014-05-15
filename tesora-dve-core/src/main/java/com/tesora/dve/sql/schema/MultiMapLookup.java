// OS_STATUS: public
package com.tesora.dve.sql.schema;

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
			capObjects.put(n, o);
		} else {
			objects.put(n, o);
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
