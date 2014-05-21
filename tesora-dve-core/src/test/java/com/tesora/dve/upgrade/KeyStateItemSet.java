// OS_STATUS: public
package com.tesora.dve.upgrade;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.util.Functional;

public class KeyStateItemSet extends StateItemSet {

	private HashMap<String, Object[]> items;

	public KeyStateItemSet(String kern) {
		super(kern);
		items = new LinkedHashMap<String,Object[]>();
	}
	
	@Override
	public void take(List<String> keyCols, List<Object> valueCols) {
		StringBuffer key = new StringBuffer();
		for(int i = 0; i < keyCols.size(); i++) {
			if (i > 0) key.append("-");
			key.append(keyCols.get(i));
		}
		items.put(key.toString(), valueCols.toArray());
	}

	@Override
	public void collectDifferences(StateItemSet other, List<String> messages) {
		KeyStateItemSet oset = (KeyStateItemSet) other;
		Map<String,Object[]> mine = items;
		Map<String,Object[]> yours = new LinkedHashMap<String,Object[]>(oset.items); 
		for(Map.Entry<String,Object[]> me : mine.entrySet()) {
			Object[] you = yours.remove(me.getKey());
			if (you == null) {
				messages.add("Missing " + kernel + " " + me.getKey());
			} else if (!Arrays.deepEquals(me.getValue(), you)) {
				StringBuilder buf = new StringBuilder();
				buf.append("Expected {");
				buf.append(buildMessage(me.getValue()));
				buf.append("}, but found {");
				buf.append(buildMessage(you));
				buf.append("}");
				messages.add(buf.toString());
			}
		}
		Collection<String> extras = yours.keySet();
		if (!extras.isEmpty()) {
			messages.add("Extra " + kernel + " found: {" + Functional.joinToString(extras, ", ") + "}");
		}

		
	}
	
}
