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
