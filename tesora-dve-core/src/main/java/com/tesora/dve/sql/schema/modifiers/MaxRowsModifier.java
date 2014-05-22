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
