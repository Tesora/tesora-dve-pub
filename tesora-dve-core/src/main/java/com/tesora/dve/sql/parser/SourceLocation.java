// OS_STATUS: public
package com.tesora.dve.sql.parser;

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
