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
