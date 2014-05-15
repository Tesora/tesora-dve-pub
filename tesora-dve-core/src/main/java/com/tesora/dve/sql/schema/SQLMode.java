// OS_STATUS: public
package com.tesora.dve.sql.schema;

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
