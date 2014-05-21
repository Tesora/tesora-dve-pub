package com.tesora.dve.sql.transform.behaviors;

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

import java.util.List;

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.DeleteFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.UpdateFeatureStep;
import com.tesora.dve.sql.util.ListSet;

public class DefaultFeatureStepBuilder implements FeatureStepBuilder {

	public static final FeatureStepBuilder INSTANCE = new DefaultFeatureStepBuilder();
	
	@Override
	public FeatureStep buildStep(PlannerContext pc, FeaturePlanner planner, 
			DMLStatement ds, DistributionKey distKey,
			DMLExplainRecord explain) {
		if (ds instanceof ProjectingStatement)
			invalid(pc.getContext(),"buildStep",ds,"use buildProjectingStep instead");
		if (ds instanceof DeleteStatement) {
			DeleteStatement del = (DeleteStatement) ds;
			List<TableInstance> tabs = del.getTargetDeletes();
			if (tabs.size() > 1)
				invalid(pc.getContext(),"buildStep",ds,"found multiple (" + tabs.size() + ") delete tables");
			DeleteFeatureStep dfs = new DeleteFeatureStep(planner, del,tabs.get(0).getTableKey(),
					tabs.get(0).getAbstractTable().getStorageGroup(pc.getContext()), distKey);
			if (explain != null)
				dfs.withExplain(explain);
			return dfs;
		} else if (ds instanceof UpdateStatement) {
			UpdateStatement us = (UpdateStatement) ds;
			ListSet<TableKey> tabs = new ListSet<TableKey>();
			for(ExpressionNode en : us.getUpdateExpressionsEdge()) {
				FunctionCall fc = (FunctionCall) en;
				tabs.addAll(ColumnInstanceCollector.getTableKeys(fc.getParametersEdge().get(0)));
			}
			TableKey tk = ensureColocation(pc.getContext(), tabs);
			if (tk == null)
				invalid(pc.getContext(),"buildStep",ds,"found multiple (" + tabs.size() + ") update tables");
			UpdateFeatureStep ufs = new UpdateFeatureStep(planner, us, tk,
					tk.getAbstractTable().getStorageGroup(pc.getContext()), distKey);
			if (explain != null)
				ufs.withExplain(explain);
			return ufs;
		} else if (ds instanceof InsertIntoSelectStatement) {
			InsertIntoSelectStatement iiss = (InsertIntoSelectStatement) ds;
			UpdateFeatureStep ufs = new UpdateFeatureStep(planner, iiss, iiss.getTableInstance().getTableKey(),
					iiss.getTableInstance().getAbstractTable().getStorageGroup(pc.getContext()), distKey);
			if (explain != null)
				ufs.withExplain(explain);
			return ufs;
		} else {
			invalid(pc.getContext(),"buildStep",ds,"unhandled");
			return null;
		}
	}

	private TableKey ensureColocation(SchemaContext sc, ListSet<TableKey> tabs) {
		if (tabs.size() == 1)
			return tabs.get(0);
		for(TableKey tk : tabs) {
			if (!tk.getAbstractTable().getDistributionVector(sc).isBroadcast())
				return null;
		}
		return tabs.get(0);
	}
	
	@Override
	public ProjectingFeatureStep buildProjectingStep(
			PlannerContext pc,
			FeaturePlanner planner,
			ProjectingStatement statement, ExecutionCost cost,
			PEStorageGroup group, Database<?> db, DistributionVector vector,
			DistributionKey distKey,
			DMLExplainRecord explain) {
		if (vector == null)
			vector = EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(statement, pc.getContext());
		ProjectingFeatureStep pfs = new ProjectingFeatureStep(pc, planner, statement, cost, group, distKey,
				db, vector);
		if (explain != null)
			pfs.withExplain(explain);
		return pfs;
	}

	private void invalid(SchemaContext sc, String method, DMLStatement statement, String reason) {
		StringBuilder buf = new StringBuilder();
		buf.append("Invalid ")
			.append(getClass().getSimpleName()).append(".").append(method)
			.append(" call: ").append(reason).append(", stmt='");
		buf.append(statement.getSQL(sc, "")).append("'");
		throw new SchemaException(Pass.PLANNER, buf.toString());
	}
	
}
