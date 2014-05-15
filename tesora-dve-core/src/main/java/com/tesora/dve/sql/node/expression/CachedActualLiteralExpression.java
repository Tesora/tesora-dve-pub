// OS_STATUS: public
package com.tesora.dve.sql.node.expression;


import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;

public class CachedActualLiteralExpression implements ILiteralExpression {

	private final Object value;
	private final int type;

	public CachedActualLiteralExpression(int valueType, Object value) {
		this.type = valueType;
		this.value = value;
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		return value;
	}

	@Override
	public int getPosition() {
		return 0;
	}

	@Override
	public boolean isParameter() {
		return false;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return this;
	}

	@Override
	public boolean isNullLiteral() {
		return type == TokenTypes.NULL;
	}

	@Override
	public boolean isStringLiteral() {
		return type == TokenTypes.Character_String_Literal;
	}

	@Override
	public int getValueType() {
		return type;
	}

	@Override
	public UnqualifiedName getCharsetHint() {
		return null;
	}

}
