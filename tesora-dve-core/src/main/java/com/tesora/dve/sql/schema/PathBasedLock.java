// OS_STATUS: public
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
