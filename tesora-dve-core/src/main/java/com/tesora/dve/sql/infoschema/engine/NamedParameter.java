// OS_STATUS: public
package com.tesora.dve.sql.infoschema.engine;

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


import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

// a hibernate style named parameter (i.e. :foo)
public class NamedParameter extends ExpressionNode {

	protected Name parameterName;
	
	public NamedParameter(Name name) {
		super((SourceLocation)null);
		parameterName = name;
	}
	
	public Name getName() {
		return parameterName;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new NamedParameter(parameterName);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		NamedParameter onp = (NamedParameter) other;
		return parameterName.equals(onp.parameterName);
	}

	@Override
	protected int selfHashCode() {
		return parameterName.hashCode();
	}

}
