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

public enum ColumnModifierKind {
	
	NULLABLE("NULL",false),
	NOT_NULLABLE("NOT NULL",false),
	DEFAULTVALUE("DEFAULT",false),
	AUTOINCREMENT("AUTOINCREMENT",false),
	ONUPDATE("ON UPDATE",true),
	// front end only modifier, used to handle inline key decls
	INLINE_KEY("unused",false);
	
	private final String sql;
	private final boolean storeAsTypeModifier;
	
	private ColumnModifierKind(String sql, boolean fake) {
		this.sql = sql;
		this.storeAsTypeModifier = fake;
	}

	public String getSQL() { return this.sql; }
	
	public boolean isStoreAsTypeModifier() {
		return storeAsTypeModifier;
	}
	
}