// OS_STATUS: public
package com.tesora.dve.exceptions;

public class PELockAbortedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public PELockAbortedException() {
		super();
	}

	public PELockAbortedException(String message, Throwable cause) {
		super(message, cause);
	}

	public PELockAbortedException(String message) {
		super(message);
	}

	public PELockAbortedException(Throwable cause) {
		super(cause);
	}

}
