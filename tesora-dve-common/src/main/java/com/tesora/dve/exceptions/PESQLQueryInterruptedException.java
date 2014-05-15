// OS_STATUS: public
package com.tesora.dve.exceptions;

public class PESQLQueryInterruptedException extends PESQLException {

	private static final String MSG = "Query execution was interrupted";
	private static final long serialVersionUID = 1L;

	public PESQLQueryInterruptedException(Throwable t) {
		super(MSG, t);
	}

	public PESQLQueryInterruptedException() {
		super(MSG);
	}

}
