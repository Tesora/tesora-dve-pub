// OS_STATUS: public
package com.tesora.dve.sql.parser;

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
