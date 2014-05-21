package com.tesora.dve.sql.schema.cache;

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


public class PersistentCacheKey implements EntityCacheKey {

	private final int id;
	private final Class<?> pclass;
	
	public PersistentCacheKey(Class<?> pc, int id) {
		this.pclass = pc;
		this.id = id;
	}
	
	@Override
	public String toString() {
		return "PersistentCacheKey{" + pclass.getSimpleName() + ":" + id + "}";
	}
	
	public Class<?> getPersistentClass() { return pclass; }
	public int getId() { return id; }


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((pclass == null) ? 0 : pclass.hashCode());
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
		PersistentCacheKey other = (PersistentCacheKey) obj;
		if (id != other.id)
			return false;
		if (pclass == null) {
			if (other.pclass != null)
				return false;
		} else if (pclass != other.pclass)
			return false;
		return true;
	}
	
	
	
}
