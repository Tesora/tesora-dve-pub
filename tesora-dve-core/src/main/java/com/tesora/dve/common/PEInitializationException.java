// OS_STATUS: public
package com.tesora.dve.common;

public class PEInitializationException extends RuntimeException {

	public PEInitializationException(String m) {
		super(m);
	}

	public PEInitializationException(String m, Throwable t) {
		super(m, t);
	}

	private static final long serialVersionUID = 1L;

}
