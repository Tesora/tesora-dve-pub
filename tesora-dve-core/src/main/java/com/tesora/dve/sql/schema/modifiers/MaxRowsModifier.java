// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.schema.SchemaContext;

public class MaxRowsModifier extends CreateOptionModifier {

	public static final long MAX_MAX_ROWS = 4294967295L;
	
	private final long maxrows;
	
	public MaxRowsModifier(long v) {
		super();
		if (v > MAX_MAX_ROWS)
			maxrows = MAX_MAX_ROWS;
		else
			maxrows = v;
	}
	
	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		buf.append("MAX_ROWS=").append(maxrows);
	}

	@Override
	public TableModifierTag getKind() {
		return TableModifierTag.MAX_ROWS;
	}

	@Override
	public String persist() {
		return "max_rows=" + maxrows;
	}

}
