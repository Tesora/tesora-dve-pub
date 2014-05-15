// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.sql.schema.cache.Cacheable;
import com.tesora.dve.sql.schema.cache.SchemaEdge;

@SuppressWarnings("rawtypes")
public interface Database<T extends Table<?>> extends HasName, PersistentDatabase, Cacheable {

	public Schema<T> getSchema();
	
	public UserDatabase getPersistent(SchemaContext sc);
	
	public PEPersistentGroup getDefaultStorage(SchemaContext sc);
	
	public SchemaEdge<PEPersistentGroup> getDefaultStorageEdge();
	
	public boolean isInfoSchema();
	
}
