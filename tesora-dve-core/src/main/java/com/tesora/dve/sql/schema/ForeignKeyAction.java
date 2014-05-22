package com.tesora.dve.sql.schema;

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

public enum ForeignKeyAction {

	RESTRICT("RESTRICT","RESTRICT"),
	CASCADE("CASCADE","CASCADE"),
	SET_NULL("SET NULL","SET NULL"),
	NO_ACTION("NO ACTION","NO ACTION");
	
	private String sql;
	private String persistent;
	
	private ForeignKeyAction(String sql, String pers) {
		this.sql = sql;
		this.persistent = pers;
	}
	
	public String getSQL() {
		return this.sql;
	}
	
	public String getPersistent() {
		return this.persistent;
	}
	
	public static ForeignKeyAction fromPersistent(String in) {
		if (in == null) return NO_ACTION;
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(ForeignKeyAction fka : ForeignKeyAction.values()) {
			if (fka.getPersistent().equals(uc))
				return fka;
		}
		return null;
	}
	
	public static ForeignKeyAction fromSQL(String in) {
		if (in == null) return NO_ACTION;
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(ForeignKeyAction fka : ForeignKeyAction.values()) {
			if (fka.getSQL().equals(uc))
				return fka;
		}
		return null;
	}
};
