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

import org.antlr.runtime.TokenStream;

public class ComputedSourceLocation extends SourceLocation {

	private int lhs;
	private int lineNumber;
	private String text;
	private int type;
	
	public ComputedSourceLocation(int lhs, int lineNo, String text, int type) {
		this.lhs = lhs;
		this.lineNumber = lineNo;
		this.text = text;
		this.type = type;
	}

	@Override
	public String getText() {
		return text;
	}
	
	@Override
	public String getText(TokenStream ts) {
		return text;
	}

	@Override
	public int getType() {
		return type;
	}

	@Override
	public int getLineNumber() {
		return lineNumber;
	}

	@Override
	public int getPositionInLine() {
		return lhs;
	}

}
