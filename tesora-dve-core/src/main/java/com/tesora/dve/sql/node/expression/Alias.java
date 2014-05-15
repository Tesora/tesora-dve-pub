// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.schema.UnqualifiedName;

public abstract class Alias {

	public abstract boolean isName();

	public abstract UnqualifiedName getNameAlias();
	
	public abstract String get();
	
	public abstract String getSQL();

	public abstract NameAlias asNameAlias();
}
