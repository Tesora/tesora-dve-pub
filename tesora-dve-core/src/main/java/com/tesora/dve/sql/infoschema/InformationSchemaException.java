// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import com.tesora.dve.sql.SchemaException;

public class InformationSchemaException extends SchemaException {
	
	private static final long serialVersionUID = 1L;

	public InformationSchemaException() {
		super(Pass.INFORMATION_SCHEMA);
	}

	public InformationSchemaException(String message) {
		super(Pass.INFORMATION_SCHEMA,message);
	}
	
	public InformationSchemaException(Throwable cause) {
		super(Pass.INFORMATION_SCHEMA,cause);
	}

	public InformationSchemaException(String message, Throwable cause) {
		super(Pass.INFORMATION_SCHEMA, message, cause);
	}
	
}
