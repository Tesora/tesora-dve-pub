// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.transform.CopyContext;

public class TenantIDLiteral extends DelegatingLiteralExpression {

	public TenantIDLiteral(ValueSource vs) {
		super(TokenTypes.Unsigned_Large_Integer,null, vs,0,null);
	}
	
	protected TenantIDLiteral(TenantIDLiteral other) {
		super(other);
	}

	@Override
	public Object getValue(SchemaContext sc) {
		return source.getTenantID(sc);
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		TenantIDLiteral out = new TenantIDLiteral(this);
		return out;
	}

	@Override
	public ILiteralExpression getCacheExpression() {
		return new CachedTenantIDLiteral(getValueType());
	}

	
}
