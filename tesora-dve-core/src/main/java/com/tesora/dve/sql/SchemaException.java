// OS_STATUS: public
package com.tesora.dve.sql;

public class SchemaException extends ParserException {

	private static final long serialVersionUID = 1L;

	protected SchemaException() {
		super();
	}
	
	public SchemaException(Pass p) {
		super(p);
	}

	public SchemaException(Pass p, String message) {
		super(p, message);
	}

	public SchemaException(Pass p, Throwable cause) {
		super(p, cause);
	}

	public SchemaException(Pass p, String message, Throwable cause) {
		super(p, message, cause);
	}

	
}
