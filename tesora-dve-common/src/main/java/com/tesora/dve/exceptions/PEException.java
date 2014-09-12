package com.tesora.dve.exceptions;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

public class PEException extends Exception {

	private static final long serialVersionUID = -3763883329162327143L;

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

	public <Type> Type getCause(Class<Type> causeClass) {
		Throwable t = this;
		while(t != null) {
			if (causeClass.equals(t.getClass()))
				return (Type)t;
			t = t.getCause();
		}
		return null;
	}

    public static Exception wrapThrowableIfNeeded(Throwable t){
        if (t instanceof Exception)
            return (Exception)t;
        else
            return new PEException(t);
    }

}
