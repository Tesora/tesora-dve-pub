// OS_STATUS: public
package com.tesora.dve.common.catalog;

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
