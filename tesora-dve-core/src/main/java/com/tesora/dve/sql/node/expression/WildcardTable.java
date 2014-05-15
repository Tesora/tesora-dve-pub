// OS_STATUS: public
package com.tesora.dve.sql.node.expression;


import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

public class WildcardTable extends Wildcard {

	private Name ofTable;
	private TableInstance table;
	
	public WildcardTable(Name tableName, TableInstance tab) {
		super(tableName.getOrig());
		ofTable = tableName;
		table = tab;
	}

	public Name getTableName() { return ofTable; }
	public TableInstance getTableInstance() { return table; }

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		TableInstance cti = cc.getTableInstance(table);
		if (cti == null) cti = (TableInstance) table.copy(cc);
		return new WildcardTable(ofTable, cti);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		WildcardTable owct = (WildcardTable) other;
		return table.getTableKey().equals(owct.table.getTableKey());
	}

	@Override
	protected int selfHashCode() {
		return table.getTableKey().hashCode();
	}

	
}
