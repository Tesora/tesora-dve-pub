package com.tesora.dve.sql.schema.cache;

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
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.schema.HasName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaLookup;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class CacheAwareLookup<T extends HasName> extends SchemaLookup<NamedEdge<T>> {

	protected List<NamedEdge<T>> values = new ArrayList<NamedEdge<T>>();
	
	@SuppressWarnings("unchecked")
	public CacheAwareLookup(boolean exactMatch,
			boolean isCaseSensitive) {
		super(Collections.EMPTY_LIST, exactMatch, isCaseSensitive);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public synchronized void add(SchemaContext sc, T targ, boolean persistent) {
		values.add(new NamedEdge(StructuralUtils.buildEdge(sc,targ,persistent),targ.getName()));
		refreshBacking(values);
	}
	
	public T lookup(SchemaContext sc, Name n) {
		NamedEdge<T> already = lookup(n);
		if (already == null) return null;
		T targ = already.getEdge().get(sc);
		if (targ == null) {
			// it's been unloaded - remove already from the values and refresh backing
			synchronized(this) {
				values.remove(already);
				refreshBacking(values);
			}
		}
		return targ;
	}
	
	public List<NamedEdge<T>> getValues() {
		return values;
	}
	
	public Collection<SchemaCacheKey<?>> getCascades() {
		return Functional.apply(getValues(), new UnaryFunction<SchemaCacheKey<?>, NamedEdge<T>>() {

			@Override
			public SchemaCacheKey<?> evaluate(NamedEdge<T> object) {
				return object.getEdge().getCacheKey();
			}

		});
	}
}
