// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

import java.util.List;

import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.parser.SourceLocation;

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
	
}
