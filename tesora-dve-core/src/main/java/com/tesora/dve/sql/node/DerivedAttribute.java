// OS_STATUS: public
package com.tesora.dve.sql.node;

import com.tesora.dve.sql.schema.SchemaContext;

public abstract class DerivedAttribute<T> {

	public abstract boolean isApplicableSubject(LanguageNode ln);
	public abstract T computeValue(SchemaContext sc, LanguageNode ln);
	
	public T getValue(LanguageNode in, SchemaContext sc) {
		return in.getBlock().getValue(this, sc);
	}
	
	public boolean hasValue(LanguageNode in, SchemaContext sc) {
		return in.getBlock().hasValue(this, sc);
	}
	
}
