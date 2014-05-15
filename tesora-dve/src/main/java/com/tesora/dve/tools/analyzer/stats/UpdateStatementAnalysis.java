// OS_STATUS: public
package com.tesora.dve.tools.analyzer.stats;

import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.UpdateStatement;

public class UpdateStatementAnalysis extends StatementAnalysis<UpdateStatement> {

	public UpdateStatementAnalysis(SchemaContext sc, String sql, int freq, UpdateStatement stmt) {
		super(sc, sql, freq, stmt);
	}

	@Override
	public void visit(StatsVisitor sv) {
		super.visit(sv);
		final UpdateStatement us = getStatement();
		for (final ExpressionNode en : us.getUpdateExpressionsEdge()) {
			if (EngineConstant.FUNCTION.has(en, EngineConstant.EQUALS)) {
				final FunctionCall fc = (FunctionCall) en;
				final ExpressionNode lhs = fc.getParametersEdge().get(0);
				if (lhs instanceof ColumnInstance) {
					final ColumnInstance ci = (ColumnInstance) lhs;
					final Column<?> column = ci.getColumn();
					if (column instanceof PEColumn) {
						sv.onUpdate((PEColumn) column, frequency);
					}
				}
			}
		}
	}
}
