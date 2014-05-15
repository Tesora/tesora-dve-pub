// OS_STATUS: public
package com.tesora.dve.exceptions;

import com.tesora.dve.common.PEContext;
import com.tesora.dve.common.PEThreadContext;

public class PERuntimeException extends RuntimeException implements PEContextAwareException {

	private static final long serialVersionUID = 1L;

	private PEContext context = PEThreadContext.copy();

	public PERuntimeException() {
		super();
	}

	public PERuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public PERuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public PERuntimeException(String message) {
		super(message);
	}

	public PERuntimeException(Throwable cause) {
		super(cause);
	}

	@Override
	public PEContext getContext() {
		return context;
	}

}
