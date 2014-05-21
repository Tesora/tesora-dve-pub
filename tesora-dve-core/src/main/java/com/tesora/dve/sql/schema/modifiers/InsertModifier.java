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

import java.util.Locale;

public enum InsertModifier {
	DELAYED("DELAYED"),
	HIGH_PRIORITY("HIGH_PRIORITY"),
	LOW_PRIORITY("LOW_PRIORITY");
	
	private String sql;
	
	private InsertModifier(String sql) {
		this.sql = sql;
	}
	
	public String getSQL() {
		return this.sql;
	}
	
	public static InsertModifier fromSQL(String in) {
		if (in == null) return null;
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(InsertModifier im : InsertModifier.values()) {
			if (im.getSQL().equals(uc))
				return im;
		}
		return null;
	}
}
