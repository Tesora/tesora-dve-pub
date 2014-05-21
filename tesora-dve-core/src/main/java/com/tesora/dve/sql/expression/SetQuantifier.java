package com.tesora.dve.sql.expression;

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

public enum SetQuantifier {

	DISTINCT,
	ALL;
	
	public static SetQuantifier fromSQL(String in) {
		String l = in.trim().toUpperCase();
		for(SetQuantifier sq : SetQuantifier.values()) {
			if (sq.name().equals(l))
				return sq;
		}
		return null;
	}
	
	public String getSQL() {
		return name();
	}
	
}
