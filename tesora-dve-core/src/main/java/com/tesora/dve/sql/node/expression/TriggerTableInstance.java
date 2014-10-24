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

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.expression.TriggerTableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transform.CopyContext;

public class TriggerTableInstance extends TableInstance {

	public static final UnqualifiedName NEW = new UnqualifiedName("NEW");
	public static final UnqualifiedName OLD = new UnqualifiedName("OLD");
	
	private final boolean before;
	
	public TriggerTableInstance(Table<?> schemaTable, long node, boolean before) {
		super(schemaTable, schemaTable.getName(), before ? NEW : OLD, node, false);
		this.before = before;
	}

	public boolean isBefore() {
		return this.before;
	}
	
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		if (cc == null)
			return withHints(new TriggerTableInstance(schemaTable,node,before));
		TriggerTableInstance out = (TriggerTableInstance) cc.getTableInstance(this);
		if (out != null) return out;
		out = withHints(new TriggerTableInstance(schemaTable, node, before));
		return cc.put(this, out);
	}


	public TableKey getTableKey() {
		return new TriggerTableKey(this);
	}

	
}
