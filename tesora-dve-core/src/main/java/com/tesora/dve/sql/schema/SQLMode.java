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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SQLMode {

	private Set<String> modes;
	
	@SuppressWarnings("unchecked")
	public SQLMode(String raw) {
		if (raw == null)
			modes = Collections.EMPTY_SET;
		else {
			String traw = raw.trim();
			if ("".equals(traw))
				modes = Collections.EMPTY_SET;
			else {
				modes = new HashSet<String>();
				String[] parts = traw.split(",");
				for(String s : parts) {
					modes.add(s.trim().toLowerCase());
				}
			}
		}
	}

	public boolean isStrictMode() {
		return modes.contains("strict_trans_tables") || modes.contains("strict_all_tables");
	}
	
	public boolean isPipesAsConcat() {
		return modes.contains("pipes_as_concat") || isOracle() || isAnsi() || isDB2() || isMaxDB() || isMSSql()  || isPostgres();  
	}
	
	public boolean isAnsi() {
		return modes.contains("ansi");
	}
	
	public boolean isDB2() {
		return modes.contains("db2");
	}
	
	public boolean isMaxDB() {
		return modes.contains("maxdb");
	}
	
	public boolean isMSSql() {
		return modes.contains("mssql");
	}
	
	public boolean isOracle() {
		return modes.contains("oracle");
	}
	
	public boolean isPostgres() {
		return modes.contains("postgresql");
	}
	
	public boolean isNoAutoOnZero() {
		return modes.contains("no_auto_value_on_zero");
	}
}
