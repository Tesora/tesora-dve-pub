package com.tesora.dve.sql.schema;

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
