// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;

public class CachedAutoIncrementLiteralExpression extends
		CachedDelegatingLiteralExpression implements IAutoIncrementLiteralExpression {

	public CachedAutoIncrementLiteralExpression(int type, int offset) {
		super(type,offset,null);
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		return sc.getValueManager().getAutoincValue(sc, this);
	}

}
