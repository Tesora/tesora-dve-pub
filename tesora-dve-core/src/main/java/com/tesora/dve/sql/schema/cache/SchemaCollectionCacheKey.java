// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import java.util.Collection;
import java.util.Collections;

import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

// a collection cache key holds a collection of other objects
// we use this to provide reasonably fast access to all X where X is not held within
// a particular cache segment - so all storage groups, all persistent sites, all providers
@SuppressWarnings("serial")
public abstract class SchemaCollectionCacheKey<T extends Persistable<?,?>> extends SchemaCacheKey<Collection<SchemaCacheKey<T>>> {

	protected SchemaCollectionCacheKey() {
		super();
	}
	
	@Override
	public final CacheSegment getCacheSegment() {
		return CacheSegment.UNCATEGORIZED;
	}

	@Override
	public final Collection<SchemaCacheKey<?>> getCascades(Object obj) {
		return Collections.emptySet();
	}
	
	@Override
	public final Collection<SchemaCacheKey<T>> load(SchemaContext sc) {
		Collection<T> actual = find(sc);
		return Functional.apply(actual, new UnaryFunction<SchemaCacheKey<T>,T>() {
			@SuppressWarnings("unchecked")
			@Override
			public SchemaCacheKey<T> evaluate(T object) {
				return (SchemaCacheKey<T>) object.getCacheKey();
			}
		});
		
	}
	
	protected abstract Collection<T> find(SchemaContext sc);
	
	public Collection<T> resolve(final SchemaContext sc) {
		Collection<SchemaCacheKey<T>>  keys = sc.getSource().find(sc, this);
		return Functional.apply(keys, new UnaryFunction<T, SchemaCacheKey<T>>() {

			@Override
			public T evaluate(SchemaCacheKey<T> object) {
				return sc.getSource().find(sc, object);
			}
			
		});
	}
}
