package com.tesora.dve.sql.node;

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

import java.util.EnumSet;

public class OffsetEdgeName implements IEdgeName {

	protected int offset;
	protected final EdgeName partOf;
	
	public OffsetEdgeName(EdgeName of, int position) {
		partOf = of;
		offset = position;
	}

	@Override
	public boolean isOffset() {
		return true;
	}

	@Override
	public String toString() {
		return super.toString() + "[" + offset + "]";
	}

	@Override
	public String getName() {
		return toString();
	}

	@Override
	public boolean any(EnumSet<EdgeName> set) {
		return set.contains(partOf);
	}
	
	@Override
	public boolean matches(IEdgeName in) {
		if (!in.isOffset()) return false;
		if (!baseMatches(in)) return false;
		OffsetEdgeName oth = (OffsetEdgeName) in;
		return offset == oth.offset;
	}

	@Override
	public boolean baseMatches(IEdgeName in) {
		return partOf == in.getBase();
	}

	@Override
	public OffsetEdgeName makeOffset(int i) {
		throw new MigrationException("Invalid call to OffsetEdgeName.makeOffset");
	}

	@Override
	public IEdgeName getBase() {
		return partOf;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + offset;
		result = prime * result + ((partOf == null) ? 0 : partOf.hashCode());
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
		OffsetEdgeName other = (OffsetEdgeName) obj;
		if (offset != other.offset)
			return false;
		if (partOf != other.partOf)
			return false;
		return true;
	}

	
}
