package com.tesora.dve.sql.expression;

import com.tesora.dve.sql.parser.LexicalLocation;

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

public enum ScopeParsePhase {
	// projection
	UNRESOLVING(LexicalLocation.PROJECTION),
	// from clause
	RESOLVING_CURRENT(LexicalLocation.ONCLAUSE), 
	// where clause
	RESOLVING(LexicalLocation.WHERECLAUSE),
	// group by clause
	GROUPBY(LexicalLocation.GROUPBYCLAUSE),            
	// having clause
	HAVING(LexicalLocation.HAVINGCLAUSE),             
	// order by clause
	TRAILING(LexicalLocation.ORDERBYCLAUSE);           
	
	private final LexicalLocation location;
	
	private ScopeParsePhase(LexicalLocation errLoc) {
		this.location = errLoc;
	}
	
	public LexicalLocation getLocation() {
		return this.location;
	}
}