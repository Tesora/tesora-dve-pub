// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.PEAbstractTable;

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
		
}
