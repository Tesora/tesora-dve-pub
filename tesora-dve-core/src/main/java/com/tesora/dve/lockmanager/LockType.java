// OS_STATUS: public
package com.tesora.dve.lockmanager;

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

public enum LockType {

	// exclusive lock request - no reads or writes
	EXCLUSIVE(3),
	// read shared - selects
	RSHARED(1),
	// write shared - inserts, updates, insert into select, deletes
	WSHARED(2);

	private final int rank;
	
	private LockType(int r) {
		rank = r;
	}

	// return true if this rank is strictly stronger than the other one.
	public boolean isStrongerThan(LockType o) {
		return rank > o.rank;
	}	
}
