// OS_STATUS: public
package com.tesora.dve.sql.parser;

import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;

public class TokenSourceLocation extends SourceLocation {

	private static final long serialVersionUID = 1L;
	private Token token;
	
	public TokenSourceLocation (Token tok) {
		token = tok;
	}

	@Override
	public String getText() {
		return token.getText();
	}

	@Override
	public String getText(TokenStream ts) {
		// may not be quite correct
		return token.getText();
	}

	@Override
	public int getType() {
		return token.getType();
	}

	@Override
	public int getLineNumber() {
		return token.getLine();
	}

	@Override
	public int getPositionInLine() {
		return token.getCharPositionInLine();
	}
	
	
	
}
