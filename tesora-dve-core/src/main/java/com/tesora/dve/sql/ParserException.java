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
import com.tesora.dve.exceptions.PEMappedRuntimeException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.variables.KnownVariables;

public class ParserException extends PEMappedRuntimeException {

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
	
	
	protected ParserException() {
		super(new ErrorInfo(DVEErrors.INTERNAL,"(unknown error)"));
	}
	
	public ParserException(Pass p) {
		super(new ErrorInfo(DVEErrors.INTERNAL,"(unknown error)"));
	}

	public ParserException(Pass p, String message) {
		super(new ErrorInfo(DVEErrors.INTERNAL,message),message);
	}

	public ParserException(Pass p, Throwable cause) {
		super(new ErrorInfo(DVEErrors.INTERNAL,cause.getMessage()),cause);
	}

	public ParserException(Pass p, String message, Throwable cause) {
		super(new ErrorInfo(DVEErrors.INTERNAL, message),message, cause);
	}

	public ParserException(ErrorInfo ei) {
		super(ei);
	}

	@Override
	public boolean hasLocation() {
		// ugh, what a freaking hack, but I don't want to add a callback everywhere
		SchemaContext current = SchemaContext.threadContext.get();
		return (current != null && current.getCatalog().isPersistent() &&
				KnownVariables.ERROR_MIGRATOR.getGlobalValue(null));
	}
		
}
