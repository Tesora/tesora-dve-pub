// OS_STATUS: public
package com.tesora.dve.exceptions;

import com.tesora.dve.common.PEContext;
import com.tesora.dve.common.PEThreadContext;

public class PEException extends Exception implements PEContextAwareException {

	private static final long serialVersionUID = -3763883329162327143L;

	private PEContext context = PEThreadContext.copy();

	protected PEException() {
		super();
	}

	public PEException(String m) {
		super(m);
	}

	public PEException(String m, Exception e) {
		super(m, e);
	}

	public PEException(String m, Throwable t) {
		super(m, t);
	}

	public PEException(Throwable t) {
		super(t);
	}

	public Exception rootCause() {
		// if (getCause() instanceof PEException)
        Throwable cause = getCause();
		if (cause != null && (cause instanceof PEException))
			return ((PEException) cause).rootCause();
		else if (cause != null && (cause instanceof Exception))
			return (Exception) cause;
		return this;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(getClass().getSimpleName());
		sb.append(": ").append(getLocalizedMessage());
		return sb.toString();
	}

	public boolean hasCause(Class<?> causeClass) {
		boolean hasClassAsCause = false;
		Throwable t = this;
		while (!hasClassAsCause && t != null) {
			hasClassAsCause = causeClass.equals(t.getClass());
			t = t.getCause();
		}
		return hasClassAsCause;
	}

	@Override
	public PEContext getContext() {
		return context;
	}
	
}
