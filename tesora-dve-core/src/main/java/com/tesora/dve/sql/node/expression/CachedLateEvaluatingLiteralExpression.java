// OS_STATUS: public
package com.tesora.dve.sql.node.expression;



import com.tesora.dve.sql.node.expression.LateEvaluatingLiteralExpression.LateEvaluator;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.ILateEvalLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;

public class CachedLateEvaluatingLiteralExpression implements
		ILateEvalLiteralExpression {

	private final int type;
	private final LateEvaluator evaluator;
	private final IConstantExpression[] expressions;
	
	public CachedLateEvaluatingLiteralExpression(int type, LateEvaluator eval, IConstantExpression[] params) {
		this.type = type;
		evaluator = eval;
		expressions = params;
	}
	
	@Override
	public int getPosition() {
		return 0;
	}

	@Override
	public ILiteralExpression getCacheExpression() {
		return this;
	}

	@Override
	public boolean isNullLiteral() {
		return false;
	}

	@Override
	public boolean isStringLiteral() {
		return this.type == TokenTypes.Character_String_Literal;
	}

	@Override
	public int getValueType() {
		return this.type;
	}

	@Override
	public UnqualifiedName getCharsetHint() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(SchemaContext sc) {
		return evaluator.getValue(sc, expressions);	
	}

	@Override
	public boolean isParameter() {
		return false;
	}

}
