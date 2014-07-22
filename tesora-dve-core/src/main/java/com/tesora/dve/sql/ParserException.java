package com.tesora.dve.sql;

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

import java.io.Serializable;

import com.tesora.dve.errmap.DVEErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PERuntimeException;

public class ParserException extends PERuntimeException {

	// TODO: change to Component
	public enum Pass implements Serializable {
		// lexer/first ast construction
		FIRST, 
		// resolution/internal tree construction
		SECOND, 
		// normalization
		NORMALIZE,
		// error in the emitter
		EMITTER,
		// or in the lexer
		LEXER,
		// rewriter
		REWRITER,
		// planner
		PLANNER,
		INFORMATION_SCHEMA
	}
	
	private static final long serialVersionUID = 1L;
	private final ErrorInfo error;
	
	
	protected ParserException() {
		super();
		error = null;
	}
	
	public ParserException(Pass p) {
		super();
		error = null;
	}

	public ParserException(Pass p, String message) {
		super(message);
		error = new ErrorInfo(DVEErrors.INTERNAL,message);
	}

	public ParserException(Pass p, Throwable cause) {
		super(cause);
		error = new ErrorInfo(DVEErrors.INTERNAL,cause.getMessage());
	}

	public ParserException(Pass p, String message, Throwable cause) {
		super(message, cause);
		error = new ErrorInfo(DVEErrors.INTERNAL, message);
	}

	public ParserException(ErrorInfo ei) {
		super();
		error = ei;
	}
	
	public ErrorInfo getErrorInfo() {
		return error;
	}
	
}
