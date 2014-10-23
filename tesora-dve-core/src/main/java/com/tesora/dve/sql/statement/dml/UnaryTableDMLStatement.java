package com.tesora.dve.sql.statement.dml;

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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TriggerEvent;

public abstract class UnaryTableDMLStatement extends DMLStatement {

	protected SingleEdge<UnaryTableDMLStatement, TableInstance> intoTable =
			new SingleEdge<UnaryTableDMLStatement, TableInstance>(UnaryTableDMLStatement.class, this, EdgeName.TABLES);

	protected UnaryTableDMLStatement(SourceLocation location) {
		super(location);
	}
	
	public TableInstance getTableInstance() { return intoTable.get(); }
	public PEAbstractTable<?> getTable() { return getTableInstance().getAbstractTable(); }

	@Override
	public List<TableInstance> getBaseTables() {
		return Collections.singletonList(intoTable.get());
	}
		
	@Override
	public boolean hasTrigger(SchemaContext sc) {
		TriggerEvent event = getTriggerEvent();
		if (event == null) return false;
		if (getTable().isView()) return false;
		if (getTable().isVirtualTable()) return false;
		return getTable().asTable().hasTrigger(sc, event);
	}
	
}
