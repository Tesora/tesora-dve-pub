// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.schema.Database;

public class PlanCacheKey {
	
	protected String shrunk;
	protected Database<?> db;
	
	public PlanCacheKey(String shrunk, Database<?> db) {
		this.shrunk = shrunk;
		this.db = db;
	}

	public Database<?> getDatabase() { return db; }

	public String getShrunk() { return shrunk; }
	
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("Database/").append(getDatabase().getName())
			.append("/").append(shrunk);
		return buf.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((db == null) ? 0 : db.getName().hashCode());
		result = prime * result + ((shrunk == null) ? 0 : shrunk.hashCode());
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
		PlanCacheKey other = (PlanCacheKey) obj;
		if (db == null) {
			if (other.db != null)
				return false;
		} else if (!db.getName().equals(other.db.getName()))  
			return false;
		if (shrunk == null) {
			if (other.shrunk != null)
				return false;
		} else if (!shrunk.equals(other.shrunk))
			return false;
		return true;
	}
	
}