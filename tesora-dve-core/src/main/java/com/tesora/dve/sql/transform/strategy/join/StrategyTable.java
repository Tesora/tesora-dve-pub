// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

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