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

import java.util.List;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.util.ListSet;

class StrategyTable {
	
	protected PartitionEntry entry;
	protected PEStorageGroup group;
	protected ListSet<TableKey> tables;
	
	public StrategyTable(PartitionEntry e, PEStorageGroup g, TableKey t) {
		entry = e;
		group = g;
		tables = new ListSet<TableKey>();
		tables.add(t);
	}
	
	public StrategyTable(PartitionEntry e, PEStorageGroup g, List<TableKey> tabs) {
		entry = e;
		group = g;
		tables = new ListSet<TableKey>();
		tables.addAll(tabs);
	}
	
	public PartitionEntry getEntry() { return entry; }
	public PEStorageGroup getGroup() { return group; }
	public TableKey getSingleTable() { return tables.get(0); }
	
	public boolean isMulti() {
		return tables.size() > 1;
	}
}