// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.schema.SchemaContext;

public class ChecksumModifier extends CreateOptionModifier {

	private final int value;
	
	public ChecksumModifier(int v) {
		super();
		value = v;
	}
	
	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		buf.append("CHECKSUM=").append(value);
	}

	@Override
	public TableModifierTag getKind() {
		return TableModifierTag.CHECKSUM;
	}

	@Override
	public String persist() {
		return "checksum=" + value;
	}

}
