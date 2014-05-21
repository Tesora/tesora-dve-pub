// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
