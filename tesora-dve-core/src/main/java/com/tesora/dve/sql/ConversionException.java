// OS_STATUS: public
package com.tesora.dve.sql;


public class ConversionException extends ParserException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected ConversionException() {
		super();
	}
	
	public ConversionException(Pass p) {
		super(p);
	}

	public ConversionException(Pass p, String message) {
		super(p, message);
	}

	public ConversionException(Pass p, Throwable cause) {
		super(p, cause);
	}

	public ConversionException(Pass p, String message, Throwable cause) {
		super(p, message, cause);
	}

}
