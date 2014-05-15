// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.types.Type;

public abstract class ConstantExpression extends ExpressionNode implements IConstantExpression {


	protected ConstantExpression(SourceLocation tree) {
		super(tree);
	}
	
	protected ConstantExpression(ConstantExpression ce) {
		super(ce);
	}
	
	@Override
	public abstract Object getValue(SchemaContext sc);

	public abstract Object convert(SchemaContext sc, Type type);

	
	

}
