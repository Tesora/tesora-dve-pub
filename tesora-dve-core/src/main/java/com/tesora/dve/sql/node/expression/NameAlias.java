// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.schema.UnqualifiedName;

public class NameAlias extends Alias {

	protected UnqualifiedName alias;
	
	public NameAlias(UnqualifiedName un) {
		alias = un;
	}

	@Override
	public boolean isName() {
		return true;
	}

	@Override
	public UnqualifiedName getNameAlias() {
		return alias;
	}

	@Override
	public String getSQL() {
		return alias.getSQL();
	}

	@Override
	public String get() {
		return alias.getUnquotedName().get();
	}

	@Override
	public NameAlias asNameAlias() {
		return this;
	}
}
