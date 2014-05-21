// OS_STATUS: public
package com.tesora.dve.sql.transform.constraints;

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

public enum PlanningConstraintType {

	// ordered by suitability
	PRIMARY(1,true),
	UNIQUE(2,true),
	REGULAR(3,false),
	DISTVECT(4,false);
	
	private final int score;
	private final boolean unique;
	private PlanningConstraintType(int s, boolean u) {
		score = s;
		unique = u;
	}
	
	public int getScore() {
		return score;
	}
	
	public boolean isUnique() {
		return unique;
	}
}
