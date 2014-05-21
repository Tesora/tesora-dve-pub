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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.variable.VariableAccessor;

public class LateResolvingVariableExpression extends ConstantExpression {

	private final VariableAccessor accessor;
	
	public LateResolvingVariableExpression(VariableAccessor va) {
		super((SourceLocation)null);
		accessor = va;
	}
	
	public VariableAccessor getAccessor() {
		return accessor;
	}
	
	@Override
	public int getPosition() {
		// doesn't have a position, because it doesn't exist until very late
		return -1;
	}

	@Override
	public boolean isParameter() {
		return false;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return null;
	}

	@Override
	public Object getValue(SchemaContext sc) {
		try {
			return sc.getConnection().getVariableValue(accessor);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to obtain late resolving variable value",pe);
		}
	}

	@Override
	public Object convert(SchemaContext sc, Type type) {
		throw new SchemaException(Pass.PLANNER, "Illegal use of late resolving variable");
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new LateResolvingVariableExpression(accessor);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		throw new SchemaException(Pass.PLANNER, "Illegal use of schemaSelfEqual");
	}

	@Override
	protected int selfHashCode() {
		throw new SchemaException(Pass.PLANNER, "Illegal use of schemaHashCode");
	}

}
