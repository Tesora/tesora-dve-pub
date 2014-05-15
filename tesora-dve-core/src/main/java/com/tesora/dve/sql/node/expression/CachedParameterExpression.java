// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.IParameter;

public class CachedParameterExpression implements IParameter {

	private int position;
	
	public CachedParameterExpression(int pos) {
		position = pos;
	}

	@Override
	public Object getValue(SchemaContext sc) {
		return sc.getValueManager().getValue(sc, this);
	}

	@Override
	public int getPosition() {
		return position;
	}

	@Override
	public boolean isParameter() {
		return true;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return this;
	}
	
}
