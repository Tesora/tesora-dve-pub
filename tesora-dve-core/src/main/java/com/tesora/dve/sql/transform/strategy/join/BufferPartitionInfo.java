// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

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

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.util.ListSet;

class BufferPartitionInfo {

	private MultiMap<BufferEntry, DPart> partitionsForEntries;
	private PartitionLookup lookup;
	
	public BufferPartitionInfo(PartitionLookup pl) {
		lookup = pl;
		partitionsForEntries = new MultiMap<BufferEntry, DPart>();
	}
	
	public void add(BufferEntry be, TableKey tk) {
		partitionsForEntries.put(be, lookup.getPartitionFor(tk));
	}
	
	public void add(BufferEntry be, ListSet<TableKey> tk) {
		partitionsForEntries.put(be, lookup.getPartitionsFor(tk));
	}

	public void add(BufferEntry be, Collection<DPart> parts) {
		partitionsForEntries.put(be, parts);
	}
	
	public List<DPart> getPartitionsForEntry(BufferEntry be) {
		return (List<DPart>)partitionsForEntries.get(be);
	}
	
	public <T extends BufferEntry> ListSet<T> filter(List<T> on, DPart p) {
		ListSet<T> out = new ListSet<T>();
		for(T be : on) {
			if (isOfPartition(be, p))
				out.add(be);
		}
		return out;
	}

	public boolean isOfPartition(BufferEntry be, DPart p) {
		List<DPart> uses = (List<DPart>) partitionsForEntries.get(be);
		if (uses == null || uses.size() != 1) return false;
		return uses.get(0) == p;
	}
	
}