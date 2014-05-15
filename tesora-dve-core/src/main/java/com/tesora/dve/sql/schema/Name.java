// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.parser.TokenTypes;

public abstract class Name implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	protected SourceLocation orig;
	
	protected Name(SourceLocation tok) {
		orig = tok;
	}
	
	protected Name() {
		orig = null;
	}
		
	public SourceLocation getOrig() {
		return orig;
	}
	
	public abstract String get();
	public abstract String getQuoted();
	
	public abstract boolean isQuoted();
	
	public abstract boolean isQualified();
	
	public abstract String getSQL();
	
	public abstract Name getCapitalized();
	public abstract Name getQuotedName();
	public abstract Name getUnquotedName();
	
	public abstract UnqualifiedName getUnqualified();

	public abstract List<UnqualifiedName> getParts();

	@Override
	public abstract int hashCode();
	@Override
	public abstract boolean equals(Object obj);
	
	public Name prefix(Name sub) {
		ArrayList<UnqualifiedName> parts = new ArrayList<UnqualifiedName>();
		parts.addAll(getParts());
		parts.addAll(sub.getParts());
		return new QualifiedName(parts);
	}

	public Name postfix(Name sub) {
		ArrayList<UnqualifiedName> parts = new ArrayList<UnqualifiedName>();
		parts.addAll(sub.getParts());
		parts.addAll(getParts());
		return new QualifiedName(parts);		
	}
	
	@Override
	public String toString() {
		return getSQL();
	}

	public boolean isAsterisk() {
		if (getOrig() == null) return false;
		return (getOrig().getType() == TokenTypes.Asterisk);
	}
	
	public void prepareForSerialization() {
		orig = null;
	}
	
	public abstract Name copy();
}
