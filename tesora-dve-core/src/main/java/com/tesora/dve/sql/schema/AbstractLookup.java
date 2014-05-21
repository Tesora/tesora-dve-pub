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

import java.util.Arrays;
import java.util.Collection;

import com.tesora.dve.sql.util.UnaryFunction;

public abstract class AbstractLookup<T extends Object> {

	protected boolean exact;
	protected boolean caseSensitive;
	protected UnaryFunction<Name[], T> mapper = null;
	
	public AbstractLookup(boolean exactMatch, boolean isCaseSensitive, UnaryFunction<Name[], T> mapBy) {
		exact = exactMatch;
		caseSensitive = isCaseSensitive;		
		mapper = mapBy;
	}
	
	@SuppressWarnings("unchecked")
	public void add(T obj) {
		refreshBacking(Arrays.asList(obj), true);
	}
	
	protected abstract void store(boolean cap, Name n, T o);
	protected abstract T get(boolean cap, Name n);

	
	protected abstract void allocateBacking(boolean cap, boolean append);

	public void add(Collection<T> objs) {
		refreshBacking(objs, true);
	}
	
	public void refreshBacking(Collection<T> objs) {
		refreshBacking(objs, false);
	}
	
	protected void refreshBacking(Collection<T> objs, boolean append) {
		allocateBacking(false,append);
		if (!caseSensitive) allocateBacking(true,append);
		if (objs != null) {
			for (T o : objs) {
				Name[] names = mapper.evaluate(o);
				for (Name n : names) {
					store(false,n,o);
					if (!caseSensitive) {
						Name cap = n.getCapitalized();
						if (cap != null)
							store(true,n.getCapitalized(),o);
					}
				}
			}
		}
	}

	public T lookup(String s) {
		return lookup(new UnqualifiedName(s));
	}
	
	public T lookup(Name n) {
		T so = get(false,n);
		if (so != null || exact) return so;
		if (!caseSensitive) {
			Name cap = n.getCapitalized();
			so = get(true,cap);
		}
		return so;
	}

}
