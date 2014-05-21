// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.ActualLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;

/*
 * When the source select of a redist includes a null literal we are unable to
 * create the temp table because we cannot determine the type of the column.  Work around
 * this by removing the column.
 */
public class NullLiteralColumnTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.NULL_LITERAL;
	}

	private static void addNullLiteralColumnToProjection(SchemaContext sc, SelectStatement ss,
			Map<Integer, ExpressionNode> nullLiterals) {
		List<ExpressionNode> newProj = new ArrayList<ExpressionNode>();
		newProj.addAll(ss.getProjection());
		for (Integer index : nullLiterals.keySet()) {
			newProj.add(index, nullLiterals.get(index));
		}
		ss.setProjection(newProj);
		// call this to get the metadata updated
		ss.normalize(sc);
	} 
	
	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (ipc.getApplied().contains(FeaturePlannerIdentifier.INSERT_INTO_SELECT) ||
				ipc.getApplied().contains(FeaturePlannerIdentifier.UNION))
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		Map<Integer, ExpressionNode> nullLiterals = new TreeMap<Integer, ExpressionNode>();

		if (stmt instanceof SelectStatement) {
			SelectStatement ss = (SelectStatement) stmt;
			List<ExpressionNode> nodes = ss.getProjection();
			for (int i = 0; i < nodes.size(); i++) {
				ExpressionNode node = nodes.get(i);
				if (node instanceof ExpressionAlias) {
					ExpressionAlias ci = (ExpressionAlias) node;
					ExpressionNode c = ci.getTarget();
					if (c instanceof ActualLiteralExpression) {
						ActualLiteralExpression ale = (ActualLiteralExpression) c;
						if (ale.isNullLiteral()) {
							nullLiterals.put(i, node);
						}
					}
				}
			}
		}
		if (nullLiterals.isEmpty())
			return null;

		SelectStatement pin = (SelectStatement) stmt;
		SelectStatement copy = CopyVisitor.copy(pin);

		List<ExpressionNode> newProj = new ArrayList<ExpressionNode>();
		for (ExpressionNode node : copy.getProjection()) {
			boolean nullLiteral = false;
			for (ExpressionNode nullNode : nullLiterals.values()) {
				if (node.isSchemaEqual(nullNode)) {
					nullLiteral = true;
					break;
				}
			}
			if (!nullLiteral) {
				newProj.add(node);
			}
		}
		copy.setProjection(newProj);
		// call this to get the metadata updated
		copy.normalize(context.getContext());

		ProjectingFeatureStep sub =
				(ProjectingFeatureStep) buildPlan(copy, context.withTransform(getFeaturePlannerID()), DefaultFeaturePlannerFilter.INSTANCE);

		addNullLiteralColumnToProjection(context.getContext(),(SelectStatement)sub.getPlannedStatement(), nullLiterals);
		
		return sub;
	}
}
