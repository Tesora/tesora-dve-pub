package com.tesora.dve.sql.expression;

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

import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.util.UnaryFunction;

public class ColumnKey extends RewriteKey {

	protected TableKey table;
	protected Column<?> column;
	
	public ColumnKey(TableKey onTable, Column<?> onColumn) {
		super();
		table = onTable;
		column = onColumn;
	}

	public ColumnKey(ColumnInstance ci) {
		super();
		table = TableKey.make(ci.getTableInstance());
		column = ci.getColumn();
	}
	
	@Override
	public ColumnInstance toInstance() {
		return new ColumnInstance(column, table.toInstance());
	}
	
	public Column<?> getColumn() {
		return column;
	}
	
	public PEColumn getPEColumn() {
		return (PEColumn)column;
	}
	
	public TableKey getTableKey() {
		return table;
	}
	
	@Override
	protected int computeHashCode() {
		final int prime = 31;
		int result = 0;
		result = prime * result + ((column == null) ? 0 : column.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnKey other = (ColumnKey) obj;
		if (column == null) {
			if (other.column != null)
				return false;
		} else if (!column.equals(other.column))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "ColumnKey{" + table + "/" + column.getName().get() + "}";
	}
	
	public static final UnaryFunction<Column<?>,ColumnKey> getColumn = new UnaryFunction<Column<?>,ColumnKey>() {

		@Override
		public Column<?> evaluate(ColumnKey object) {
			return object.getColumn();
		}
		
	};
	
	public static final UnaryFunction<PEColumn,ColumnKey> getPEColumn = new UnaryFunction<PEColumn,ColumnKey>() {

		@Override
		public PEColumn evaluate(ColumnKey object) {
			return object.getPEColumn();
		}
		
	};
	
}
