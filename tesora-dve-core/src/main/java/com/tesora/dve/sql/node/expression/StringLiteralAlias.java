// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class StringLiteralAlias extends Alias {

	protected String alias;
	
	public StringLiteralAlias(String in) {
		alias = in;
	}
	
	@Override
	public boolean isName() {
		return false;
	}

	@Override
	public UnqualifiedName getNameAlias() {
		return null;
	}

	@Override
	public String getSQL() {
		return "'" + alias + "'";
	}

	@Override
	public String get() {
		return alias;
	}

	@Override
	public NameAlias asNameAlias() {
		throw new SchemaException(Pass.PLANNER, "Invalid use of string literal alias, expected name alias");
	}

}
