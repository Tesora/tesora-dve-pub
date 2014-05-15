// OS_STATUS: public
package com.tesora.dve.exceptions;


public class PENotFoundException extends PEException {

	private static final long serialVersionUID = 1L;

	protected PENotFoundException() {
		super();
	}
	
	public PENotFoundException(String m, Exception e) {
		super(m, e);
	}

	public PENotFoundException(String m, Throwable t) {
		super(m, t);
	}

	public PENotFoundException(String m) {
		super(m);
	}

}
