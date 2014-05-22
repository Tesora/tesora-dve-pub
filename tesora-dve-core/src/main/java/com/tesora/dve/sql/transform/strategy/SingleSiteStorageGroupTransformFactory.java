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



import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.VectorRange;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

/*
 * Applies if the query can execute on a single site via either storage group topology,
 * tenant distribution, or having all bcast tables
 */
public class SingleSiteStorageGroupTransformFactory extends TransformFactory {

	private static boolean disabledFlag = Boolean.getBoolean("com.tesora.dve.sql.transform.strategy.singlesite.disable");
	
	public static void disable(boolean value) {
		disabledFlag = value;
	}
	
	public static boolean isDisabled() {
		return disabledFlag;
	}
	
	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.SINGLE_SITE;
	}
	
	public static boolean isSingleSite(SchemaContext sc, DMLStatement sp, boolean ignoremt) throws PEException {
		// in the mt case, we might do order by rewrites even in the single persistent site case
		List<PEStorageGroup> groups = sp.getStorageGroups(sc);
		if (groups.size() != 1)
			return false;
		// if there are order by clauses don't apply in mt mode
		boolean isMTMode = sc.getPolicyContext().isMTMode();
		if (!ignoremt && EngineConstant.ORDERBY.has(sp) && isMTMode)
			return false;
		// check for mt where all tables are distributed on tenant column using same range
		Pair<TableKey,RangeDistribution> vr = isOnlyDistributedOnTenantColumn(sc,sp); 
		if (vr != null) {
			// annotate the statement so we can pick it out later on
			sp.getBlock().store(SingleSiteStorageGroupTransformFactory.class, vr);
			return true;			
		}		
		
		if (isNonMTAllTableBroadcastSelectStatement(sc, sp, isMTMode)) {
			return true;
		}

		if (sc.getOptions() != null && sc.getOptions().isInhibitSingleSiteOptimization()) return false;
		
		return groups.get(0).isSingleSiteGroup();
		
	}
	
	public static boolean isNonMTAllTableBroadcastSelectStatement(SchemaContext sc, DMLStatement sp,
			boolean isMTMode) {
		 if (!(sp instanceof ProjectingStatement)) {
			return false;
		 }
		
		if (isMTMode) {
			return false;
		}
		
		// if NOT MT and all the referenced tables are broadcast then we can
		// apply this transform
		for(TableKey tbl : EngineConstant.TABLES_INC_NESTED.getValue(sp, sc)) {
			if (!tbl.getAbstractTable()
					.getDistributionVector(sc)
					.isBroadcast()) {
				// not a broadcast table
				return false;
			}
		}

		// if we got here then the table(s) referenced are all broadcast so
		// let this transform handle it
		return true;
	}

	public static Pair<TableKey,RangeDistribution> isOnlyDistributedOnTenantColumn(SchemaContext sc, DMLStatement sp) {
		// check for mt where all tables are distributed on tenant column using same range
		if (sc.getPolicyContext().requiresMTRewrites(sp.getExecutionType())) {
			RangeDistribution range = null;
			RangeDistribution current = null;
			ListSet<TableKey> tables = EngineConstant.TABLES_INC_NESTED.getValue(sp,sc);
			boolean singleSite = true; // assume
			Pair<TableKey,RangeDistribution> candidate = null;
			for(TableKey tk : tables) {
				if (tk.getAbstractTable().isVirtualTable()) continue;
				current = tk.getAbstractTable().getDistributionVector(sc).getDistributedWhollyOnTenantColumn(sc);
				if (current == null) {
					singleSite = false;
					break;
				} else if (range == null) {
					range = current;
					candidate = new Pair<TableKey, RangeDistribution>(tk,range);
				} else if (!range.equals(current)) {
					singleSite = false;
					break;
				}
			}
			if (singleSite)
				return candidate;
		}
		return null;
	}
	
	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (isDisabled())
			return null;
		if (!isSingleSite(ipc.getContext(),stmt,false))
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		
		// special context that does not check invariants
		ExecutionCost cost = null;
		DMLExplainRecord record = null;
		if (context.isCosting() && stmt instanceof SelectStatement) {
			cost = Costing.buildBasicCost(context.getContext(), (SelectStatement)stmt, true);
			record = new DMLExplainRecord(DMLExplainReason.SINGLE_SITE,null,cost.getRowCount());
		} else {
			cost = new ExecutionCost(EngineConstant.WHERECLAUSE.has(stmt),true,null,-1);
			record = DMLExplainReason.SINGLE_SITE.makeRecord();
		}

		DistributionKey dk = null;
		
		FeatureStep root = null;
		@SuppressWarnings("unchecked")
		Pair<TableKey,VectorRange> any = (Pair<TableKey,VectorRange>) stmt.getBlock().getFromStorage(SingleSiteStorageGroupTransformFactory.class);
		if (any != null &&
				(context.getContext().getPolicyContext().isSchemaTenant() ||
				 context.getContext().getPolicyContext().isDataTenant())) {
			ListOfPairs<PEColumn,ConstantExpression> values = new ListOfPairs<PEColumn,ConstantExpression>();
			values.add(any.getFirst().getAbstractTable().getTenantColumn(context.getContext()), 
					context.getContext().getPolicyContext().getTenantIDLiteral(false));
			dk = 
					any.getFirst().getAbstractTable().getDistributionVector(context.getContext()).buildDistKey(context.getContext(), any.getFirst(), values);
			stmt.getBlock().clearFromStorage(SingleSiteStorageGroupTransformFactory.class);
		}
		if (stmt instanceof ProjectingStatement) {
			root = DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(context,
					this,
					(ProjectingStatement)stmt, 
					cost,
					stmt.getSingleGroup(context.getContext()),
					stmt.getDatabase(context.getContext()),
					EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(stmt,context.getContext()),
					dk,
					DMLExplainReason.SINGLE_SITE.makeRecord());
		} else {
			root = DefaultFeatureStepBuilder.INSTANCE.buildStep(context, this, stmt, dk, record);
		}

		return root.withDefangInvariants();
	}


}
