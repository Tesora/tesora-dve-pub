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


import java.util.ArrayList;
import java.util.Collections;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.DistKeyCollector;
import com.tesora.dve.sql.transform.TransformKey;
import com.tesora.dve.sql.transform.TransformKey.TransformKeySimple;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistributionFlags;
import com.tesora.dve.sql.util.Pair;

/*
 * Specifically for insert into select.  We plan as a redist with a user table as
 * the target; we may set some additional flags for autoinc processing.
 */
public class InsertIntoTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.INSERT_INTO_SELECT;
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!(stmt instanceof InsertIntoSelectStatement))
			return null;
				
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		InsertIntoSelectStatement iiss = (InsertIntoSelectStatement) stmt;
		
		SelectStatement srcSelect = null;
		if (iiss.getSource() instanceof SelectStatement)
			srcSelect = (SelectStatement) iiss.getSource();
		if (srcSelect == null)
			throw new SchemaException(Pass.PLANNER, "No transform support for insert into ... select ... union ....");
			
		PEColumn missingAutoInc = null;
		if (iiss.getColumnSpecification().size() > srcSelect.getProjection().size()) {
			for(int i = srcSelect.getProjection().size(); i < iiss.getColumnSpecification().size(); i++) {
				ColumnInstance ci = (ColumnInstance) iiss.getColumnSpecification().get(i);
				if (missingAutoInc != null)
					throw new SchemaException(Pass.PLANNER,"More than one missing autoinc column found");
				missingAutoInc = ci.getPEColumn();
			}
		}
		// if any of the target columns are existing autoincs, we also need to make the engine aware of that
		Integer existingAutoInc = null;
		for(int i = 0; i < iiss.getColumnSpecificationEdge().size(); i++) {
			ColumnInstance ci = (ColumnInstance)iiss.getColumnSpecificationEdge().get(i);
			if (ci.getPEColumn().isAutoIncrement()) {
				if (existingAutoInc != null)
					throw new SchemaException(Pass.PLANNER,"More than one existing autoinc column found");
				existingAutoInc = i;
			}
		}
		boolean mtopt = false;
		if (missingAutoInc == null && context.getContext().getPolicyContext().isMTMode()) {
			// some day this won't work if the tenants are different, but as we can only 'use' one tenant at time, ought not to matter
			RangeDistribution leftRange = iiss.getTable().getDistributionVector(context.getContext()).getDistributedWhollyOnTenantColumn(context.getContext());
			if (leftRange != null) {
				Pair<TableKey,RangeDistribution> pv = SingleSiteStorageGroupTransformFactory.isOnlyDistributedOnTenantColumn(context.getContext(), iiss.getSource());
				RangeDistribution rightRange = pv.getSecond();
				if (leftRange.equals(rightRange)) {
					mtopt = true;
				}
			}
		}
		
		// if mtopt is true apply the filter to ensure that we get a key
		if (mtopt && context.getContext().getPolicyContext().isMTMode()) {
			context.getContext().getPolicyContext().applyDegenerateMultitenantFilter(srcSelect);
		}

		ProjectingFeatureStep child = (ProjectingFeatureStep) buildPlan(srcSelect, context, DefaultFeaturePlannerFilter.INSTANCE);

		ArrayList<ColumnInstance> sourceDV = new ArrayList<ColumnInstance>();
		for(PEColumn col : iiss.getTable().getDistributionVector(context.getContext()).getColumns(context.getContext())) 
			sourceDV.add(new ColumnInstance(col,iiss.getTableInstance()));
		SelectStatement planned = (SelectStatement) child.getPlannedStatement();
		// arrange for the aliases on the planned select to match the column names of the target table
		ArrayList<UnqualifiedName> aliases = new ArrayList<UnqualifiedName>();
		for(ExpressionNode e : iiss.getColumnSpecification()) {
			ColumnInstance ci = (ColumnInstance) e;
			aliases.add(ci.getColumn().getName().getUnqualified());
		}
		// if we have any existing alias instances on the order bys/group bys - disconnect them and
		// turn them back into column refs
		for(SortingSpecification ss : planned.getGroupBysEdge()) {
			if (ss.getTarget() instanceof AliasInstance) {
				AliasInstance ai = (AliasInstance) ss.getTarget();
				ss.getTargetEdge().set(ai.getTarget().getTarget());
			}
		}
		for(SortingSpecification ss : planned.getOrderBysEdge()) {
			if (ss.getTarget() instanceof AliasInstance) {
				AliasInstance ai = (AliasInstance) ss.getTarget();
				ss.getTargetEdge().set(ai.getTarget().getTarget());					
			}
		}
		for(int i = 0; i < planned.getProjection().size(); i++) {
			ExpressionAlias ea = (ExpressionAlias) planned.getProjection().get(i);
			ea.setAlias(aliases.get(i));
		}
		if (mtopt) {
			// copy the original insert into select and paste the planned child in - we'll execute directly on the 
			// persistent site
			InsertIntoSelectStatement copy = CopyVisitor.copy(iiss);
			copy.getSourceEdge().set(planned);
			DistributionKey distKey = child.getDistributionKey();
			if (child.hasPlanner(FeaturePlannerIdentifier.SINGLE_SITE)) {
				// if it was single site, it won't have figured out the dist key (because normally this doesn't matter)
				// do that now - this is very important if this iiss is for an mt adaptive flip, because we want to 
				// limit the dml to a single node so as not to incur an xa txn, which doesn't play well with ddl (which
				// is almost always involved if this is a flip)
				SelectStatement src = CopyVisitor.copy(planned);
				DistKeyCollector dkc = new DistKeyCollector(context.getContext(),EngineConstant.WHERECLAUSE.getEdge(src));
				TransformKey tk = null;
				if (!dkc.getSeries().isEmpty()) tk = dkc.getSeries().iterator().next();
				if (!dkc.getSingles().isEmpty()) tk = dkc.getSingles().iterator().next();
				if (tk instanceof TransformKeySimple) {
					DistributionKey dk = ((TransformKeySimple)tk).buildKeyValue(context.getContext());
					distKey = dk;
				}
			}

			return DefaultFeatureStepBuilder.INSTANCE.buildStep(context, this, copy, distKey, 
					DMLExplainReason.TENANT_DISTRIBUTION.makeRecord());
		} else {
			TableInstance intoTable = iiss.getTableInstance();
			RedistributionFlags flags = new RedistributionFlags();
			if (iiss.getIgnore())
				flags.withInsertIgnore();
			if (iiss.getOnDuplicateKeyEdge().has())
				flags.withDuplicateKeyClause(iiss.getOnDuplicateKey());
			else
				flags.withRowCount(true);
			flags.withExistingAutoInc(existingAutoInc).withAutoIncColumn(missingAutoInc);
			
			// todo: figure out why we need to defang here
			return new RedistFeatureStep(this, child, 
					intoTable.getTableKey(), 
					intoTable.getTableKey().getAbstractTable().getStorageGroup(context.getContext()),
					Collections.<Integer> emptyList(),
					flags).withDefangInvariants();
		}
		
	}
	
}
