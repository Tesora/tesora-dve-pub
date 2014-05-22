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

import com.tesora.dve.common.PEConstants;

public enum PolicyClass {

	AGGREGATE(PEConstants.AGGREGATE),
	SMALL(PEConstants.SMALL),
	MEDIUM(PEConstants.MEDIUM),
	LARGE(PEConstants.LARGE);
	
	private final String persistent;
	private final String sql;
	
	private PolicyClass(String p) {
		this.persistent = p;
		this.sql = p.toUpperCase();
	}
	
	public String getSQL() {
		return this.sql;
	}
	
	public String getPersistent() {
		return persistent;
	}
	
	public static PolicyClass findSQL(String v) {
		for(PolicyClass pc : PolicyClass.values()) {
			if (pc.getSQL().equalsIgnoreCase(v))
				return pc;
		}
		return null;
	}
	
	public static PolicyClass findPersistent(String v) {
		for(PolicyClass pc : PolicyClass.values()) {
			if (pc.getPersistent().equalsIgnoreCase(v))
				return pc;
		}
		return null;
	}
}
