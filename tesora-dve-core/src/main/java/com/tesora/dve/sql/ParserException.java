// OS_STATUS: public
package com.tesora.dve.sql;

import java.io.Serializable;

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
	private final Pass phase;
	
	protected ParserException() {
		super();
		phase = null;
	}
	
	public ParserException(Pass p) {
		phase = p;
	}

	public ParserException(Pass p, String message) {
		super(message);
		phase = p;
	}

	public ParserException(Pass p, Throwable cause) {
		super(cause);
		phase = p;
	}

	public ParserException(Pass p, String message, Throwable cause) {
		super(message, cause);
		phase = p;
	}

	public Pass getPass() {
		return phase;
	}
	
}
