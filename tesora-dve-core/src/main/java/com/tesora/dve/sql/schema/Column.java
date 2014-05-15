// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.sql.schema.types.Type;

public interface Column<T extends Table<?>> extends HasName {

	public Type getType();
	
	public T getTable();
	public void setTable(T t);

	@Override
	public Name getName();
	
	public boolean isTenantColumn();
	
	public int getPosition();
}
