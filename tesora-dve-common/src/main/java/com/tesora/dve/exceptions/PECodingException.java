// OS_STATUS: public
package com.tesora.dve.exceptions;

public class PECodingException extends PERuntimeException {

	private static final long serialVersionUID = 1L;

	public PECodingException() {
	}

	public PECodingException(String message) {
		super(message);
	}

	public PECodingException(Throwable cause) {
		super(cause);
	}

	public PECodingException(String message, Throwable cause) {
		super(message, cause);
	}

}
