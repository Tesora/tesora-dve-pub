// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;


import com.tesora.dve.sql.schema.HasName;
import com.tesora.dve.sql.schema.Name;

public class NamedEdge<T> implements HasName {

	private final SchemaEdge<T> target;
	private final Name name;
	
	public NamedEdge(SchemaEdge<T> e, Name n) {
		target = e;
		name = n;
	}
	
	@Override
	public Name getName() {
		return name;
	}
	
	public SchemaEdge<T> getEdge() {
		return target;
	}

	
}
