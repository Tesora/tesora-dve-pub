// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.schema.SchemaContext;

public class CachedTenantIDLiteral extends CachedDelegatingLiteralExpression {

	public CachedTenantIDLiteral(int type) {
		super(type,0,null);
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		return sc.getValueManager().getTenantID(sc);
	}
	
}
