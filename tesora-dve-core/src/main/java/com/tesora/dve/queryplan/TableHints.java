package com.tesora.dve.queryplan;

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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.HasAutoIncrementTracker;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.util.Pair;

public class TableHints {

	public static TableHints EMPTY_HINT = new TableHints();
	
	Pair<UserColumn,HasAutoIncrementTracker> autoIncColumn;
	// in the src select, offsets of autoinc values that we need to keep track of
	Pair<Integer, HasAutoIncrementTracker> existingAutoIncs;

	public TableHints() {
		autoIncColumn = null;
	}
	
	public TableHints withMissingAutoIncs(Pair<UserColumn,HasAutoIncrementTracker> in) {
		autoIncColumn = in;
		return this;
	}
	
	public TableHints withExistingAutoIncs(Pair<Integer,HasAutoIncrementTracker> offset) {
		existingAutoIncs = offset;
		return this;
	}
	
	public ColumnSet addAutoIncMetadata(ColumnSet cs) {
		if (cs == null)
			System.out.println("BOOM!");
		ColumnSet adjustedCS = new ColumnSet(cs);
		if (tableHasAutoIncs()) {
			ColumnMetadata cm = autoIncColumn.getFirst().getColumnMetadata();
			cm.setAliasName(autoIncColumn.getFirst().getName());
			cm.setNativeTypeId(MyFieldType.FIELD_TYPE_LONGLONG.getByteValue());
			adjustedCS.addColumn(cm);
		}
		return adjustedCS;
	}

	public long[] buildBlocks(CatalogDAO catalogDAO, int howMany) {
		long[] autoIncrBlocks = null;
		if (tableHasAutoIncs()) {
			autoIncrBlocks = new long[1];
			autoIncrBlocks[0] = autoIncColumn.getSecond().getNextIncrBlock(catalogDAO, howMany);
		}
		return autoIncrBlocks;
	}

	public void maybeAddAutoIncs(ResultRow row, long[] blocks) {
		if (tableHasAutoIncs())
			row.addResultColumn(new Long(blocks[0]++), false);
	}

	public boolean tableHasAutoIncs() {
		return autoIncColumn != null;
	}
	public boolean usesExistingAutoIncs() {
		return existingAutoIncs != null;
	}
	
	public boolean isUsingAutoIncColumn() {
		return (tableHasAutoIncs() || usesExistingAutoIncs());
	}
	
	public boolean isExistingAutoIncColumn(int position) {
		return (existingAutoIncs.getFirst().intValue() == position);
	}
	
	public long maximizeExistingAutoInc(ResultRow row, long current) {
		if (usesExistingAutoIncs()) {
			Object value = row.getResultColumn(existingAutoIncs.getFirst().intValue() + 1).getColumnValue();
			if (value instanceof Number) {
				Number n = (Number) value;
				long v = n.longValue();
				if (v > current)
					return v;
			}
		}
		return current;
	}
	
	public void recordMaximalAutoInc(CatalogDAO catalogDAO, long maxValue) {
		if (existingAutoIncs != null) {
			existingAutoIncs.getSecond().removeNextIncrValue(catalogDAO, maxValue);
		}
	}
	
	public void modify(UserTable ut) {
	}
}
