// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.Collection;

import com.tesora.dve.sql.node.expression.TableInstance;

public interface Schema<T extends Table<?>> {

	public T addTable(SchemaContext sc, T t);

	public Collection<T> getTables(SchemaContext sc);
	
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n, LockInfo lockType, boolean domtchecks);
	
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n, LockInfo lockType);

	public UnqualifiedName getSchemaName(SchemaContext sc);
}
