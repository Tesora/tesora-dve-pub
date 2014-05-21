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

// index type is the type of index implementation
public enum IndexType {

	BTREE("BTREE"),
	HASH("HASH"),
	RTREE("RTREE"),
	FULLTEXT("FULLTEXT");
	
	private final String sql;
	private IndexType(String s) {
		sql = s;
	}
	
	public String getSQL() {
		return sql;
	}
	
	public String getPersistent() {
		return sql;
	}
	
	public static IndexType fromPersistent(String in) {
		for(IndexType it : IndexType.values()) {
			if (it.getSQL().equalsIgnoreCase(in))
				return it;
		}
		return null;
	}
}
