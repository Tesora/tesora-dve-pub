// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class CollationTableModifier extends TableModifier {

	private final UnqualifiedName collation;
	
	public CollationTableModifier(UnqualifiedName unq) {
		super();
		collation = unq;
	}
	
	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		buf.append("DEFAULT COLLATE ").append(collation.getSQL());
	}

	@Override
	public TableModifierTag getKind() {
		return TableModifierTag.DEFAULT_COLLATION;
	}

	public UnqualifiedName getCollation() {
		return collation;
	}
	
}
