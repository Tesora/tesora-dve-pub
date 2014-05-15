// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

public abstract class SetExpression {

	public SetExpression() {
		
	}
	
	public enum Kind {
		TRANSACTION_ISOLATION,
		VARIABLE
	}
	
	public abstract Kind getKind();
	
}
