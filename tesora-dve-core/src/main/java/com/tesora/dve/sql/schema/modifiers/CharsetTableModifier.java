// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class CharsetTableModifier extends TableModifier {

	private final UnqualifiedName name;
	
	public CharsetTableModifier(UnqualifiedName n) {
		super();
		name = n;
	}
	
	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		buf.append("DEFAULT CHARSET=").append(name.getSQL());
	}

	@Override
	public TableModifierTag getKind() {
		return TableModifierTag.DEFAULT_CHARSET;
	}

	public UnqualifiedName getCharset() {
		return name;
	}
	
}
