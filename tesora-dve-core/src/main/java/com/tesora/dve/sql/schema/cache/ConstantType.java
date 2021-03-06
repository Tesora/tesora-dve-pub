package com.tesora.dve.sql.schema.cache;

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

// kinds of constants - values that are constant for planning purposes
// but may change from execution to execution
public enum ConstantType {

	// i.e. 1, 'foo'
	LITERAL,  
	// gets filled in each time
	TENANT_LITERAL,
	// allocated each time (maybe)
	AUTOINCREMENT_LITERAL,
	// originally '?', then gets bound upon execute
	PARAMETER,
	// originally a column reference or expression, this is a value that is bound
	// at runtime (i.e. during execution in the engine)
	RUNTIME, 
	
}
