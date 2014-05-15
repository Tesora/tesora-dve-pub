// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.schema.Database;


public class ContainerPlanCacheKey extends PlanCacheKey {

	private boolean global;
	
	protected ContainerPlanCacheKey(String shrunk, Database<?> db, boolean globalContext) {
		super(shrunk, db);
		global = globalContext;
	}

	@Override
	public String toString() {
		return "Container(" + global + "):" + super.toString();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (global ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ContainerPlanCacheKey other = (ContainerPlanCacheKey) obj;
		if (global != other.global)
			return false;
		return true;
	}

}
