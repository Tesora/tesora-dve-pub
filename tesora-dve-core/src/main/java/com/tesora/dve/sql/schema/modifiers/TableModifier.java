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
