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

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;

public class SQLMode {

	public enum SQLModes {
		NO_AUTO_CREATE_USER,
		NO_ENGINE_SUBSTITUTION,
		STRICT_TRANS_TABLES,
		STRICT_ALL_TABLES,
		PIPES_AS_CONCAT,
		ANSI,
		DB2,
		MAXDB,
		MSSQL,
		ORACLE,
		POSTGRESQL,
		NO_AUTO_VALUE_ON_ZERO,
		DEFAULT;
		
		public static SQLModes find(String raw) {
			for(SQLModes m : SQLModes.values()) {
				if (raw.equals(m.name()))
					return m;
			}
			return null;
		}			
		
	}

	private Set<String> unknownModes;
	private EnumSet<SQLModes> knownModes;
	
	@SuppressWarnings("unchecked")
	public SQLMode(String raw) {
		unknownModes = new LinkedHashSet<String>();
		knownModes = EnumSet.noneOf(SQLModes.class);
		if (raw != null) {
			String traw = raw.trim();
			if (!"".equals(traw)) {
				String[] parts = traw.split(",");
				for(String s : parts) {
					String fixed = s.trim().toUpperCase();
					SQLModes m = SQLModes.find(fixed);
					if (m != null)
						knownModes.add(m);
					else
						unknownModes.add(fixed);
				}
			}
		}
	}

	public SQLMode(SQLMode o) {
		unknownModes = new LinkedHashSet<String>(o.unknownModes);
		knownModes = EnumSet.noneOf(SQLModes.class);
		knownModes.addAll(o.knownModes);
	}
	
	public String toString() {
		StringBuilder buf = new StringBuilder();
		Functional.join(knownModes, buf, ",", new BinaryProcedure<SQLModes,StringBuilder>() {

			@Override
			public void execute(SQLModes aobj, StringBuilder bobj) {
				bobj.append(aobj.name());				
			}
			
		});
		if (!unknownModes.isEmpty()) {
			if (buf.length() > 0)
				buf.append(",");
			buf.append(Functional.join(unknownModes, ","));
		}
		return buf.toString();
	}
		
	public boolean isStrictMode() {
		return knownModes.contains(SQLModes.STRICT_TRANS_TABLES) || knownModes.contains(SQLModes.STRICT_ALL_TABLES);
	}
	
	public boolean isPipesAsConcat() {
		return knownModes.contains(SQLModes.PIPES_AS_CONCAT)
				|| isOracle() || isAnsi() || isDB2() || isMaxDB() || isMSSql()  || isPostgres();  
	}
	
	public boolean isAnsi() {
		return knownModes.contains(SQLModes.ANSI);
	}
	
	public boolean isDB2() {
		return knownModes.contains(SQLModes.DB2);
	}
	
	public boolean isMaxDB() {
		return knownModes.contains(SQLModes.MAXDB);
	}
	
	public boolean isMSSql() {
		return knownModes.contains(SQLModes.MSSQL);
	}
	
	public boolean isOracle() {
		return knownModes.contains(SQLModes.ORACLE);
	}
	
	public boolean isPostgres() {
		return knownModes.contains(SQLModes.POSTGRESQL);
	}
	
	public boolean isNoAutoOnZero() {
		return knownModes.contains(SQLModes.NO_AUTO_VALUE_ON_ZERO);
	}
	
	public SQLMode add(SQLMode other) {
		SQLMode out = new SQLMode(this);
		out.knownModes.addAll(other.knownModes);
		out.unknownModes.addAll(other.unknownModes);
		return out;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((knownModes == null) ? 0 : knownModes.hashCode());
		result = prime * result
				+ ((unknownModes == null) ? 0 : unknownModes.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SQLMode other = (SQLMode) obj;
		if (knownModes == null) {
			if (other.knownModes != null)
				return false;
		} else if (!knownModes.equals(other.knownModes))
			return false;
		if (unknownModes == null) {
			if (other.unknownModes != null)
				return false;
		} else if (!unknownModes.equals(other.unknownModes))
			return false;
		return true;
	}
	
	
}
