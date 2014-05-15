// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;



import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;

public interface SchemaSource extends SchemaSourcePlanCache {

	<T> T find(SchemaContext cntxt, SchemaCacheKey<T> ck);
	
	// schema context only calls this once the object is fully loaded upto any scope or table related
	// lazy lookup
	void setLoaded(Persistable<?,?> p, SchemaCacheKey<?> sck);
	
	// must not do the lookup
	Persistable<?,?> getLoaded(SchemaCacheKey<?> sck);
	
	// a regular edge is derived from persistent state - there's an existing relationship in the
	// catalog.  if the object isn't already mapped in the cache, add it.
	@SuppressWarnings("rawtypes")
	<T> SchemaEdge buildEdge(T p);	
	
	<T> SchemaEdge<T> buildTransientEdge(T p);
	
	// build an edge on the assumption that the target cache key is already loaded
	<T> SchemaEdge<T> buildEdgeFromKey(SchemaCacheKey<T> sck);

	// the version at creation time - used to detect when we should clear the whole cache
	long getVersion();
	
	public CacheType getType();
	
}
