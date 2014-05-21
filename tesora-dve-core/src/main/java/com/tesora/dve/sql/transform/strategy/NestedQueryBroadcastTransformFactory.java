// OS_STATUS: public
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
import java.util.List;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.ComplexFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

/**
 * This transformation colocates all subqueries on a single group.
 */
public class NestedQueryBroadcastTransformFactory extends TransformFactory {

	/**
	 * Check if the parent statement has all tables (including nested) broadcast
	 * on the it's storage group.
	 */
	protected static boolean hasAllNestedCollocated(final SchemaContext sc, final DMLStatement parent) {
		final ListSet<ProjectingStatement> nestedQueries = parent.getDerivedInfo().getLocalNestedQueries();
		if (nestedQueries.isEmpty()) {
			return true;
		}
		
		final List<PEStorageGroup> storageGroups = parent.getStorageGroups(sc);
		if (storageGroups.size() != 1) {
			return false;
		}

		return hasAllTablesBroadcast(sc, parent);
	}

	private static boolean hasAllTablesBroadcast(final SchemaContext sc, final DMLStatement stmt) {
		final List<TableKey> localTables = stmt.getDerivedInfo().getLocalTableKeys();
		for (final TableKey table : stmt.getDerivedInfo().getAllTableKeys()) {
			if (!localTables.contains(table) && !table.getAbstractTable().getDistributionVector(sc).isBroadcast()) {
				return false;
			}
		}

		return true;
	}

	private static boolean applies(SchemaContext sc, DMLStatement stmt) throws PEException {
		if ((stmt instanceof UpdateStatement) || (stmt instanceof DeleteStatement)) {
			return !hasAllNestedCollocated(sc, stmt);
		}

		return false;		
	}
	
	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.NESTED_QUERY_BROADCAST;
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!applies(ipc.getContext(),stmt))
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		final DMLStatement stmtCopy = CopyVisitor.copy(stmt);
		
		final PEStorageGroup targetGroup = stmtCopy.getBaseTables().get(0).getAbstractTable().getStorageGroup(context.getContext()); 
		final ListSet<ProjectingStatement> nestedQueries = new ListSet<ProjectingStatement>(stmtCopy.getDerivedInfo().getLocalNestedQueries());

		ListOfPairs<ProjectingStatement, FeatureStep> subs = new ListOfPairs<ProjectingStatement,FeatureStep>();
		
		for (final ProjectingStatement nested : nestedQueries) {
			FeatureStep childStep = buildPlan(nested, context, DefaultFeaturePlannerFilter.INSTANCE);
			subs.add(nested,childStep);
		}

		ListSet<FeatureStep> deps = new ListSet<FeatureStep>();
		for(Pair<ProjectingStatement,FeatureStep> planned : subs) {
			ProjectingFeatureStep pfs = (ProjectingFeatureStep) planned.getSecond();
			RedistFeatureStep rfs =
					pfs.redist(context, this,
							new TempTableCreateOptions(Model.BROADCAST, targetGroup),
							null,
							null);
			SelectStatement ss = rfs.buildNewSelect(context);
			ExpressionPath replacer = ExpressionPath.build(planned.getFirst(), stmtCopy);
			replacer.update(stmtCopy, ss);
			
			stmtCopy.getDerivedInfo().getLocalNestedQueries().remove(planned.getFirst());
			stmtCopy.getDerivedInfo().addNestedStatements(Collections.singletonList((ProjectingStatement)ss));
			stmtCopy.getBlock().clear();		
			deps.add(rfs);
		}

		FeatureStep out = buildPlan(stmtCopy, context, 
				new ComplexFeaturePlannerFilter(Collections.singleton(getFeaturePlannerID()), Collections.<FeaturePlannerIdentifier> emptySet()));
		
		out.prefixChildren(deps);
		
		return out;
	}
}
