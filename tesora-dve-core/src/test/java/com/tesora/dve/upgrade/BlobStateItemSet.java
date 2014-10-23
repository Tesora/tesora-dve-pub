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

	@Override
	public boolean isEmpty() {
		return blobs.isEmpty();
	}

}
