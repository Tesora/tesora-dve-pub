// OS_STATUS: public
package com.tesora.dve.sql.jg;

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

public class JoinPosition implements Comparable<JoinPosition> {

	// -1 if a where clause join, branch elsewise
	final int branch;
	final int offset;
	final boolean fixed;
	
	public JoinPosition(int b, int o, boolean fix) {
		branch = b;
		offset = o;
		fixed = fix;
	}
	
	public JoinPosition() {
		this(-1,-1,false);
	}

	public boolean isInformal() {
		return branch == -1;
	}
	
	@Override
	public String toString() {
		return "(" + branch + "," + offset + ")";
	}
	
	public int getBranch() {
		return branch;
	}
	
	public int getOffset() {
		return offset;
	}
	
	public boolean isFixed() {
		return fixed;
	}
	
	@Override
	public int compareTo(JoinPosition o) {
		if (getBranch() < o.getBranch())
			return -1;
		if (getBranch() > o.getBranch())
			return 1;
		if (getOffset() < o.getOffset())
			return -1;
		if (getOffset() > o.getOffset())
			return 1;
		return 0;
	}
	
}
