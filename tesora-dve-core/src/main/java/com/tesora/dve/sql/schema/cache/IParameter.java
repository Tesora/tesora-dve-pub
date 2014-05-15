// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

public interface IParameter extends IConstantExpression {

	// left to right in the original expression
	@Override
	public int getPosition();
	
}
