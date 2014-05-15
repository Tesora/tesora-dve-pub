// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class StringLiteralAlias extends Alias {

	protected String alias;
	
	public StringLiteralAlias(String in) {
		alias = in;
	}
	
	@Override
	public boolean isName() {
		return false;
	}

	@Override
	public UnqualifiedName getNameAlias() {
		return null;
	}

	@Override
	public String getSQL() {
		return "'" + alias + "'";
	}

	@Override
	public String get() {
		return alias;
	}

	@Override
	public NameAlias asNameAlias() {
		throw new SchemaException(Pass.PLANNER, "Invalid use of string literal alias, expected name alias");
	}

}
