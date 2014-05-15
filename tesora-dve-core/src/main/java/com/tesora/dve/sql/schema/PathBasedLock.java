// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.Arrays;

import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.sql.util.Functional;

public abstract class PathBasedLock implements LockSpecification {

	private final String[] path;
	private final String key;
	private final String reason;
	
	public PathBasedLock(String reason, String...vals) {
		this.reason = reason;
		path = vals;
		key = Functional.join(Arrays.asList(path), "/");
	}
	
	@Override
	public String getOriginator() {
		return reason;
	}

    @Override
	public String getName() {
		return key;
	}

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
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
		PathBasedLock other = (PathBasedLock) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	public String toString() {
		return getName() + " because " + getOriginator();
	}
	
}
