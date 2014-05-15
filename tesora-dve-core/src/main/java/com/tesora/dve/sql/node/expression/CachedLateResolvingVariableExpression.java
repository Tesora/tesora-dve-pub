// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.variable.VariableAccessor;

public class CachedLateResolvingVariableExpression implements IConstantExpression {

	private final VariableAccessor accessor;
	
	public CachedLateResolvingVariableExpression(VariableAccessor va) {
		accessor = va;
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		try {
			return sc.getConnection().getVariableValue(accessor);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to obtain variable value",pe);
		}
	}

	@Override
	public int getPosition() {
		return -1;
	}

	@Override
	public boolean isParameter() {
		return false;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return this;
	}

}
