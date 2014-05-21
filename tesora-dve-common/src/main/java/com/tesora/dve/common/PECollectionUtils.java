// OS_STATUS: public
package com.tesora.dve.common;

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

import java.util.Collection;
import java.util.List;

public final class PECollectionUtils {
	
	private PECollectionUtils() {
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T selectRandom(Collection<T> col) {
		int selected = (int) (Math.random() * col.size());
		return (T) col.toArray()[selected];
	}

	public static <T> T selectRandom(List<T> col) {
		int selected = (int) (Math.random() * col.size());
		return col.get(selected);
	}

}
