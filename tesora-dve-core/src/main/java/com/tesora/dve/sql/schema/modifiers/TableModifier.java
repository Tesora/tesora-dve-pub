// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.expression.Traversable;
import com.tesora.dve.sql.schema.SchemaContext;

public abstract class TableModifier extends Traversable {

	public TableModifier() {
		super();
	}
	
	public abstract void emit(SchemaContext sc, Emitter emitter, StringBuilder buf);
	
	public abstract TableModifierTag getKind();
	
	public boolean isUnknown() {
		return getKind() == null;
	}

	public boolean isCreateOption() {
		return false;
	}
}
