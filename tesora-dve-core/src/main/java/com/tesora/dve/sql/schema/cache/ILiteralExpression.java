// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.schema.UnqualifiedName;

public interface ILiteralExpression extends IConstantExpression {

	public boolean isNullLiteral();
	
	public boolean isStringLiteral();
	
	public int getValueType();
	
	public UnqualifiedName getCharsetHint();
	

}
