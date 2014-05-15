// OS_STATUS: public
package com.tesora.dve.sql.util;

import java.util.HashMap;
import java.util.Map;

public abstract class Options<K extends Enum<K>> {

	private Map<K, Object> options;
	
	protected Options() {
		this.options = new HashMap<K, Object>();
	}
	
	protected Options(Options<K> other) {
		this.options = new HashMap<K, Object>(other.options);
	}
	
	protected abstract Options<K> copy();
	
	protected Options<K> addSetting(K variety, Object v) {
		Options<K> ret = copy();
		ret.options.put(variety, v);
		return ret;
	}
	
	protected Object getSetting(K variety) {
		return options.get(variety);
	}
	
	protected boolean hasSetting(K variety) {
		Object any = getSetting(variety);
		if (any instanceof Boolean) {
			Boolean b = (Boolean)any;
			return b.booleanValue();
		} else {
			return (any != null);
		}
	}
}
