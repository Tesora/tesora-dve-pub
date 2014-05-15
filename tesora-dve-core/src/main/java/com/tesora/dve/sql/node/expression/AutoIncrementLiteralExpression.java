// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.transform.CopyContext;

public class AutoIncrementLiteralExpression extends DelegatingLiteralExpression implements IAutoIncrementLiteralExpression {

	public AutoIncrementLiteralExpression(ValueManager vm, int position) {
		super(TokenTypes.Unsigned_Large_Integer,null,vm,position,null);
	}
	
	protected AutoIncrementLiteralExpression(DelegatingLiteralExpression dle) {
		super(dle);
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		return source.getAutoincValue(sc, this);
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new AutoIncrementLiteralExpression(this);
	}

	@Override
	public ILiteralExpression getCacheExpression() {
		return new CachedAutoIncrementLiteralExpression(getValueType(), position);
	}

}
