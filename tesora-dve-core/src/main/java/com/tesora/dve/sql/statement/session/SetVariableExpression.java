package com.tesora.dve.sql.statement.session;

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



import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.schema.SchemaContext;

public class SetVariableExpression extends SetExpression {

	private final VariableInstance variable;
	// the expression - either a single expression node with an arbitrary value,
	// or else a list of identifiers
	private final List<ExpressionNode> value;
	
	public SetVariableExpression(VariableInstance var, ExpressionNode value) {
		super();
		variable = var;
		this.value = Collections.singletonList(value);
	}
	
	public SetVariableExpression(VariableInstance var, List<ExpressionNode> rhs) {
		super();
		variable = var;
		value = rhs;
	}
	
	public VariableInstance getVariable() {
		return variable;
	}
	
	public List<ExpressionNode> getValue() {
		return this.value;
	}

	public ExpressionNode getVariableExpr() {
		if (value.size() == 1)
			return value.get(0);
		return null;
	}
	
	public Object getVariableValue(SchemaContext sc) {
		ExpressionNode e = getVariableExpr();
		if (e == null)
			return null;
		// try convert to a literal.
		if (e instanceof LiteralExpression) {
			LiteralExpression le = (LiteralExpression) e;
			if (le.isNullLiteral()) return null;
			return le.getValue(sc);
		} else {
			throw new SchemaException(Pass.PLANNER, "Unable to convert variable value to literal: " + e);
		}
	}
 	
	@Override
	public Kind getKind() {
		return Kind.VARIABLE;
	}

	// it is simple if the rhs is basically constant
	public boolean isSimple() {
		ExpressionNode e = getVariableExpr();
		if (e == null) return true;
		if (e instanceof LiteralExpression)
			return true;
		return false;
	}
	
}
