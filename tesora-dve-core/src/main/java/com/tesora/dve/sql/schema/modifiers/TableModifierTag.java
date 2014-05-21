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

public enum TableModifierTag {
	
	// declaration order is the order in which we emit these
	ENGINE,
	AUTOINCREMENT,
	DEFAULT_CHARSET,
	DEFAULT_COLLATION,
	MAX_ROWS(true),
	CHECKSUM(true),
	ROW_FORMAT(true),
	COMMENT;

	private final boolean createOption;
	
	private TableModifierTag(boolean co) {
		createOption = co;
	}
	
	private TableModifierTag() {
		this(false);
	}
	
	public final boolean isCreateOption() {
		return createOption;
	}
	
}