// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;

public class IdentifierLiteralExpression extends ActualLiteralExpression {

	public IdentifierLiteralExpression(Name n) {
		super(n, 0, n.getOrig(),null);
	}

	@Override
	public Object getValue(SchemaContext sc) {
		Name n = (Name)super.getValue(sc);
		return n.getSQL();
	}
	
	@Override
	public Object getValue() {
		Name n = (Name) super.getValue();
		return n.getSQL();
	}
}
