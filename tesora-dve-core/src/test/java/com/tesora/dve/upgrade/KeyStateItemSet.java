// OS_STATUS: public
package com.tesora.dve.upgrade;

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
