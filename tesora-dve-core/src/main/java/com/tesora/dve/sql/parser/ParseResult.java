// OS_STATUS: public
package com.tesora.dve.sql.parser;

import java.util.List;

import com.tesora.dve.sql.statement.Statement;

public class ParseResult {
	
	private List<Statement> statements;
	// when the inputPosition == null - the whole input was consumed
	private final InputState inputPosition;
	
	public ParseResult(List<Statement> stmts, InputState state) {
		statements = stmts;
		inputPosition = state;
	}
	
	public List<Statement> getStatements() {
		return statements;
	}

	public boolean hasMore() {
		return inputPosition != null;
	}
	
	public InputState getInputState() {
		return inputPosition;
	}
}