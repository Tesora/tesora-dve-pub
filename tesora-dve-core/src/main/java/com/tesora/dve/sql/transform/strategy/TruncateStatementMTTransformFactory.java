// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.Collections;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

/**
 * Convert the statement to tenant-wise deletes in MT mode.
 */
public class TruncateStatementMTTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.TRUNCATE_MT_TABLE;
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		final TruncateStatement ts = (TruncateStatement) stmt;
		final TableInstance targetTable = ts.getTruncatedTable();
		if (!targetTable.isMT())
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		final DeleteStatement transformedStmt = new DeleteStatement(
				Collections.singletonList(new FromTableReference(targetTable)),
				null,
				true,
				null);
		transformedStmt.getDerivedInfo().take(stmt.getDerivedInfo());
		transformedStmt.getDerivedInfo().addLocalTable(targetTable.getTableKey());

		return buildPlan(transformedStmt, context, DefaultFeaturePlannerFilter.INSTANCE);
	}

}
