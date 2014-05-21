// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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

import java.util.List;

import com.tesora.dve.sql.schema.UnqualifiedName;

public class IndexHint {

	private List<UnqualifiedName> indexes;
	private HintTarget target;
	private HintType type;
	private boolean isKey;
	
	public IndexHint(HintType type, boolean key, HintTarget target, List<UnqualifiedName> indexNames) {
		this.type = type;
		this.target = target;
		this.isKey = key;
		this.indexes = indexNames;
	}
		
	public HintTarget getHintTarget() {
		return target;
	}
	
	public HintType getHintType() {
		return type;
	}
	
	public boolean isKey() {
		return isKey;
	}
	
	public List<UnqualifiedName> getIndexNames() {
		return indexes;
	}
	
	public enum HintType {
		
		USE("USE"),
		IGNORE("IGNORE"),
		FORCE("FORCE");
		
		private final String sql;
		
		private HintType(String s) {
			sql = s;
		}
		
		public String getSQL() {
			return sql;
		}
		
	}
	
	public enum HintTarget {
		
		JOIN("JOIN"),
		ORDERBY("ORDER BY"),
		GROUPBY("GROUP BY");
		
		private final String sql;
		
		private HintTarget(String s) {
			sql = s;
		}
		
		public String getSQL() {
			return sql;
		}
	}
	
	
}
