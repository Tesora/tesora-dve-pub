// OS_STATUS: public
package com.tesora.dve.sql.statement.session;



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
