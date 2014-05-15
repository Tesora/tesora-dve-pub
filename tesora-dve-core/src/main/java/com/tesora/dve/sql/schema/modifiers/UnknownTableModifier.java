// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import java.util.List;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;

// an unknown table modifier is a pass through
public class UnknownTableModifier extends TableModifier {

	private List<ExpressionNode> exprs;

	public UnknownTableModifier(List<ExpressionNode> exprs) {
		this.exprs = exprs;
	}
	
	public List<ExpressionNode> getExprs() {
		return this.exprs;
	}

	@Override
	public void emit(SchemaContext sc, Emitter emitter, StringBuilder buf) {
		boolean first = true;
		for(ExpressionNode e : exprs) {
			if (first) first = false;
			else
				buf.append(" ");
			emitter.emitExpression(sc,e, buf);
		}
	}

	@Override
	public TableModifierTag getKind() {
		return null;
	}


}
