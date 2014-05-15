// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;

public interface ValueSource {

	public Object getValue(SchemaContext sc, IParameter p);
	
	public Object getLiteral(SchemaContext sc, IDelegatingLiteralExpression dle);
	
	public Object getTenantID(SchemaContext sc);
	
	public Object getAutoincValue(SchemaContext sc, IAutoIncrementLiteralExpression exp); 
	
}
