// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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
