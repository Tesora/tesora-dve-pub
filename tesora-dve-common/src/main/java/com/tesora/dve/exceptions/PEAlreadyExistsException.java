// OS_STATUS: public
package com.tesora.dve.exceptions;


public class PEAlreadyExistsException extends PEException {

	private static final long serialVersionUID = 1L;

	protected PEAlreadyExistsException() {
		super();
	}
	
	public PEAlreadyExistsException(String m, Exception e) {
		super(m, e);
	}

	public PEAlreadyExistsException(String m, Throwable t) {
		super(m, t);
	}

	public PEAlreadyExistsException(String m) {
		super(m);
	}
}
