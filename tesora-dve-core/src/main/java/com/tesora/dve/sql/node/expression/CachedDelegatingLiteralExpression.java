// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;

public class CachedDelegatingLiteralExpression implements IDelegatingLiteralExpression {

	protected int tokenType;
	protected int position;
	protected UnqualifiedName charsetHint;
	
	public CachedDelegatingLiteralExpression(int type, int offset, UnqualifiedName cshint) {
		this.tokenType = type;
		this.position = offset;
		this.charsetHint = cshint;
	}
	
	@Override
	public UnqualifiedName getCharsetHint() {
		return charsetHint;
	}
	
	@Override
	public boolean isNullLiteral() {
		return tokenType == TokenTypes.NULL;
	}

	@Override
	public boolean isStringLiteral() {
		return tokenType == TokenTypes.Character_String_Literal;
	}

	@Override
	public Object getValue(SchemaContext sc) {
		return sc.getValueManager().getLiteral(sc, this);
	}

	@Override
	public int getPosition() {
		return position;
	}

	@Override
	public int getValueType() {
		return tokenType;
	}

	@Override
	public ILiteralExpression getCacheExpression() {
		return this;
	}

	@Override
	public boolean isParameter() {
		return false;
	}

}
