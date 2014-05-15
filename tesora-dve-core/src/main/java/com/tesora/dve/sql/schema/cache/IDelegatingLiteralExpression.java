// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

public interface IDelegatingLiteralExpression extends ILiteralExpression {

	@Override
	public int getPosition();
	
	@Override
	public ILiteralExpression getCacheExpression();
}
