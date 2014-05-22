package com.tesora.dve.common.catalog;

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

public enum KeyType {

	// so all the keys we can represent are:
	// primary key
	// index
	// key
	// fulltext index
	// fulltext key
	// foreign key
	
	// the persistent names are driven by info schema requirements.   sorry.
	PRIMARY("PRIMARY",true),
	INDEX("INDEX",false),
	KEY("KEY",true),
	FOREIGN("FOREIGN",true),
	FULLTEXT_KEY("FT_KEY",true),
	FULLTEXT_INDEX("FT_INDEX",false),
	// fake key - this represents a distribution key
	// i.e. any dist vect that uses columns would have one of these
	DISTRIBUTION("UNUSED",false,false);
		
	private final String persistentName;
	private final boolean isKey;
	private final boolean isSQL;
	
	private KeyType(String pname, boolean key) {
		this(pname,key,true);
	}
	
	private KeyType(String pname, boolean key, boolean sql) {
		persistentName = pname;
		isKey = key;
		isSQL = sql;
	}
	
	public String getPersistentName() {
		return persistentName;
	}
	
	public boolean isKey() {
		return isKey;
	}
	
	public boolean isSQL() {
		return isSQL;
	}
	
	public static KeyType decode(String in) {
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(KeyType kt : KeyType.values()) {
			if (kt.getPersistentName().equals(uc))
				return kt;
		}
		return null;
	}
}
