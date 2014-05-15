// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.schema.SchemaContext;

public interface IConstantExpression {

	public Object getValue(SchemaContext sc);

	public int getPosition();

	public boolean isParameter();
	
	public IConstantExpression getCacheExpression();
	
}
