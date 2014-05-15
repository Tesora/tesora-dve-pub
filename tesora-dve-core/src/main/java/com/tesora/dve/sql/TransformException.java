// OS_STATUS: public
package com.tesora.dve.sql;

public class TransformException extends ParserException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected TransformException() {
		super();
	}
	
	public TransformException(Pass p) {
		super(p);
	}

	public TransformException(Pass p, String message) {
		super(p, message);
	}

	public TransformException(Pass p, Throwable cause) {
		super(p, cause);
	}

	public TransformException(Pass p, String message, Throwable cause) {
		super(p, message, cause);
	}

}
