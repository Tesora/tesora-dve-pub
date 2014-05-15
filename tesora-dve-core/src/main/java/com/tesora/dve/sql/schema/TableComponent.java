// OS_STATUS: public
package com.tesora.dve.sql.schema;

public interface TableComponent<T> {

	// look up the component represented by this object in tab
	public T getIn(SchemaContext sc, PEAbstractTable<?> tab);
	
	// the targ object has a different definition than this one, take the new definition
	public void take(SchemaContext sc, T tc);
	
}
