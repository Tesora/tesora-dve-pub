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

import java.util.List;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;

public abstract class MultiTableDMLStatement extends DMLStatement {

	protected MultiEdge<MultiTableDMLStatement, FromTableReference> tableReferences =
			new MultiEdge<MultiTableDMLStatement, FromTableReference>(MultiTableDMLStatement.class, this, EdgeName.TABLES);	
	protected SingleEdge<MultiTableDMLStatement, ExpressionNode> whereClause =
			new SingleEdge<MultiTableDMLStatement, ExpressionNode>(MultiTableDMLStatement.class, this, EdgeName.WHERECLAUSE);

	protected MultiTableDMLStatement(SourceLocation location) {
		super(location);
	}
	
	public List<FromTableReference> getTables() { return tableReferences.getMulti(); }
	public MultiEdge<MultiTableDMLStatement, FromTableReference> getTablesEdge() { return tableReferences; }

	public MultiTableDMLStatement setTables(List<FromTableReference> refs) { 
		tableReferences.set(refs);
		return this;
	}
	
	public ExpressionNode getWhereClause() { return whereClause.get(); }
	public MultiTableDMLStatement setWhereClause(ExpressionNode en) {
		whereClause.set(en);
		return this;
	}

	@Override
	public List<TableInstance> getBaseTables() {
		return computeBaseTables(tableReferences.getMulti());
	}	
	
	public boolean supportsPartitions() {
		return true;
	}
	
	@Override
	public boolean hasTrigger(SchemaContext sc) {
		if (this instanceof ProjectingStatement) return false;
		for(TableKey tk : getDerivedInfo().getLocalTableKeys()) {
			if (tk.getAbstractTable().isView()) continue;
			if (tk.getAbstractTable().isVirtualTable()) continue;
			if (tk.getAbstractTable().asTable().hasTrigger(sc, getStatementType()))
				return true;
		}
		return false;
	}

}
