// OS_STATUS: public
package com.tesora.dve.exceptions;


public class PESQLException extends PEException {

	protected PESQLException() {
		super();
	}
	
	public PESQLException(String m, Exception e) {
		super(m, e);
	}

	public PESQLException(String m, Throwable e) {
		super(m, e);
	}

	public PESQLException(Throwable throwable) {
		super(throwable);
	}

	public PESQLException(String m) {
		super(m);
	}

	private static final long serialVersionUID = 1L;

}
