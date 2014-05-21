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
import org.antlr.runtime.tree.CommonTree;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;

public abstract class SourceLocation implements Comparable<SourceLocation>, java.io.Serializable {

	private static final long serialVersionUID = 1L;

	protected SourceLocation() {}
	
	public abstract String getText();
	
	public abstract String getText(TokenStream ts);
	
	public abstract int getType();
	
	public abstract int getLineNumber();
	public abstract int getPositionInLine();
	
	@Override
	public int compareTo(SourceLocation other) {
		int left = getLineNumber();
		int right = other.getLineNumber();
		if (left < right) return -1;
		if (left > right) return 1;
		left = getPositionInLine();
		right = other.getPositionInLine();
		if (left < right) return -1;
		if (left > right) return 1;
		return 0;
	}
	
	public static SourceLocation make(Token tok) {
		return new TokenSourceLocation(tok);
	}

	public static SourceLocation make(CommonTree ct) {
		return new TreeSourceLocation(ct);
	}
	
	public static SourceLocation make(Object obj) {
		if (obj == null) return null;
		if (obj instanceof Token)
			return make((Token)obj);
		else if (obj instanceof CommonTree)
			return make((CommonTree)obj);
		else if (obj instanceof SourceLocation)
			return (SourceLocation)obj;
		else
			throw new SchemaException(Pass.FIRST, "Unknown source loc source: " + obj.getClass().getName());
	}
	
	@Override
	public String toString() {
		return getLineNumber() + ":" + getPositionInLine();
	}
	
}
