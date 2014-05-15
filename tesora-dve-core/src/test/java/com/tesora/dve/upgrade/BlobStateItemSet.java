// OS_STATUS: public
package com.tesora.dve.upgrade;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.sql.util.Functional;

public class BlobStateItemSet extends StateItemSet {

	private LinkedHashSet<String> blobs;
	
	public BlobStateItemSet(String kern) {
		super(kern);
		blobs = new LinkedHashSet<String>();
	}
	
	@Override
	public void take(List<String> keyCols, List<Object> valueCols) {
		StringBuffer buf = new StringBuffer();
		for(int i = 0; i < valueCols.size(); i++) {
			if (i > 0) buf.append("-");
			Object o = valueCols.get(i);
			buf.append(o == null ? "null" : o);
		}
		blobs.add(buf.toString());
	}

	@Override
	public void collectDifferences(StateItemSet other,
			List<String> messages) {
		BlobStateItemSet oblob = (BlobStateItemSet) other;
		Set<String> mine = blobs;
		Set<String> yours = new LinkedHashSet<String>(oblob.blobs);
		for(Iterator<String> iter = mine.iterator(); iter.hasNext();) {
			String m = iter.next();
			if (!yours.contains(m)) {
				messages.add("Missing " + kernel + " " + m);				
			} else {
				yours.remove(m);
			}
		}
		if (!yours.isEmpty()) {
			messages.add("Extra " + kernel + " found: {" + Functional.joinToString(yours, ", ") + "}");
		}		
	}

}
