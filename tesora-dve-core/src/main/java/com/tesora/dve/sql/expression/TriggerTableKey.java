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
import com.tesora.dve.sql.node.expression.TriggerTableInstance;
import com.tesora.dve.sql.schema.Table;

public class TriggerTableKey extends TableKey {

	protected boolean before;
	
	public TriggerTableKey(TriggerTableInstance ti) {
		super(ti);
		this.before = ti.isBefore();
	}

	public TriggerTableKey(Table<?> backing, long n, boolean before) {
		super(backing,n);
		this.before = before;
	}
	
	public boolean isBefore() {
		return before;
	}
	
	@Override
	public TableInstance toInstance() {
		if (instance == null)
			instance = new TriggerTableInstance(backingTable, node, before);
		return instance;
	}
	

}
