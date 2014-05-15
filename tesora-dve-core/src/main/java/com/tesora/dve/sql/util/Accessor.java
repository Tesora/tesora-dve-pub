// OS_STATUS: public
package com.tesora.dve.sql.util;

public abstract class Accessor<FieldType, ContainingType> extends UnaryFunction<FieldType, ContainingType> {

	public FieldType get(ContainingType ct) throws Exception {
		return evaluate(ct);
	}
	
}
