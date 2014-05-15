// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.List;

public interface Table<T extends Column<?>> extends HasName {

	
	public T addColumn(SchemaContext sc, T c);
	
	public List<T> getColumns(SchemaContext sc);
	
	public T lookup(SchemaContext sc, Name n);
	
	public Name getName(SchemaContext sc);

	public boolean isInfoSchema();
	
	public Database<?> getDatabase(SchemaContext sc);
}
