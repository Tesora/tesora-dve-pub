// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class ReplaceIntoSelectStatement extends InsertIntoSelectStatement {

	public ReplaceIntoSelectStatement(TableInstance tab, List<ExpressionNode> columns,
			ProjectingStatement ss, boolean nestedGrouped, AliasInformation aliasInfo, SourceLocation loc) {
		super(tab, columns, ss, nestedGrouped, null, aliasInfo, loc);
	}

	@Override
	public boolean isReplace() {
		return true;
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence ges, BehaviorConfiguration config) throws PEException {
		TableInstance ti = getTableInstance();
		Table<?> table = ti.getTable();
		if (table.isInfoSchema()) 
			throw new PEException("Cannot insert into info schema table " + intoTable.get().getTable().getName());
		DMLStatement.planViaTransforms(sc, this, ges, config);		
	}
	
}
