// OS_STATUS: public
package com.tesora.dve.sql.util;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MapOfMaps<OuterKey, InnerKey, Value> {

	private Map<OuterKey, Map<InnerKey, Value>> backing;
	
	public MapOfMaps() {
		backing = new HashMap<OuterKey, Map<InnerKey, Value>>();
	}
	
	public Map<InnerKey, Value> createSubmap() {
		return new HashMap<InnerKey, Value>();
	}
	
	public Value put(OuterKey ok, InnerKey ik, Value nv) {
		Map<InnerKey, Value> sub = get(ok);
		if (sub == null) {
			sub = createSubmap();
			backing.put(ok, sub);
		}
		return sub.put(ik, nv);
	}
	
	public Map<InnerKey, Value> get(OuterKey ok) {
		return backing.get(ok);
	}
	
	public Value get(OuterKey ok, InnerKey ik) {
		Map<InnerKey, Value> sub = get(ok);
		if (sub == null)
			return null;
		return sub.get(ik);
	}
	
	public Set<OuterKey> keySet() {
		return backing.keySet();
	}
	
	public void clear() {
		backing.clear();
	}
	
}
