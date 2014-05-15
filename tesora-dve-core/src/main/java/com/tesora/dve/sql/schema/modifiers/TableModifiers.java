// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import java.util.List;

public class TableModifiers {

	private TableModifier[] modifiers;
	
	public TableModifiers() {
		modifiers = new TableModifier[TableModifierTag.values().length];
	}
	
	public TableModifiers(List<TableModifier> in) {
		this();
		if (in == null) return;
		for(TableModifier tm : in)
			setModifier(tm);
	}
	
	public void setModifier(TableModifier tm) {
		modifiers[tm.getKind().ordinal()] = tm;
	}
	
	public TableModifier getModifier(TableModifierTag tag) {
		return modifiers[tag.ordinal()];
	}

	public TableModifier[] getModifiers() {
		return modifiers;
	}
}
