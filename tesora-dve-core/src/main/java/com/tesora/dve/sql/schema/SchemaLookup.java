// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.Collection;

import com.tesora.dve.sql.util.UnaryFunction;

public class SchemaLookup<SchemaObject extends HasName> extends Lookup<SchemaObject> {

	public SchemaLookup(Collection<SchemaObject> declOrder, boolean exactMatch, boolean isCaseSensitive) {
		super(declOrder, new UnaryFunction<Name[], SchemaObject>() {

			@Override
			public Name[] evaluate(SchemaObject object) {
				return new Name[] { object.getName() };
			}
			
		}
				,exactMatch,isCaseSensitive);
	}
	
	public void refresh(Collection<SchemaObject> declOrder) {
		refreshBacking(declOrder);
	}
	
}
