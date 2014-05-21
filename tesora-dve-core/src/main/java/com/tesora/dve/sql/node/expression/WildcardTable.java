package com.tesora.dve.sql.node.expression;

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
