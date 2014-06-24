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

import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;

public class TemporaryTableKey extends TableKey {

	TemporaryTableKey(Table<?> backing, long n) {
		super(backing, n);
		// TODO Auto-generated constructor stub
	}

	TemporaryTableKey(TableInstance ti) {
		super(ti);
	}
	
	private ComplexPETable getComplexTable() {
		return (ComplexPETable) backingTable;
	}
	
	@Override
	public long getNextAutoIncrBlock(SchemaContext sc, long blockSize) {
		return getComplexTable().getNextAutoIncrBlock(sc, blockSize);
	}

	@Override
	public long readAutoIncrBlock(SchemaContext sc) {
		return getComplexTable().readAutoIncrBlock(sc);
	}

	@Override
	public void removeValue(SchemaContext sc, long value) {
		getComplexTable().removeValue(sc, value);
	}

	@Override
	public TableKey makeFrozen() {
		return new TemporaryTableKey(backingTable,node);
	}

	@Override
	public boolean isUserlandTemporaryTable() {
		return true;
	}

}
