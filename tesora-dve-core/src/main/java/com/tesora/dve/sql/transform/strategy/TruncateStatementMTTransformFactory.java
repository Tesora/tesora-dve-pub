package com.tesora.dve.sql.transform.strategy;

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
