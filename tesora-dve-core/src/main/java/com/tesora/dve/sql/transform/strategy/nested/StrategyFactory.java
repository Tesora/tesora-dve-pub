// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.nested;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public abstract class StrategyFactory {

	public abstract NestingStrategy adapt(SchemaContext sc, EdgeTest location, DMLStatement enclosing, Subquery sq, ExpressionPath path)
		throws PEException;
	
}
