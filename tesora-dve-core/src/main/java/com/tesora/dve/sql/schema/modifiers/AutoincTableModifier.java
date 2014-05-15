// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.schema.SchemaContext;

public class AutoincTableModifier extends TableModifier {

	private final long startAt;

	public AutoincTableModifier(long v) {
		super();
		startAt = v;
	}
	
	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		buf.append("AUTO_INCREMENT").append("=").append(startAt);
	}

	@Override
	public TableModifierTag getKind() {
		return TableModifierTag.AUTOINCREMENT;
	}
	
	public long getStartAt() {
		return startAt;
	}
	
	
}
