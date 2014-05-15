// OS_STATUS: public
package com.tesora.dve.sql.util;

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
