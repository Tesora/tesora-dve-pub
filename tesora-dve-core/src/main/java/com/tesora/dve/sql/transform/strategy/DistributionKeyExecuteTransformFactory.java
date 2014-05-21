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


import java.util.Collection;
import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ConstraintCollector;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.DistKeyCollector;
import com.tesora.dve.sql.transform.TransformKey;
import com.tesora.dve.sql.transform.TransformKey.TransformKeySeries;
import com.tesora.dve.sql.transform.TransformKey.TransformKeySimple;
import com.tesora.dve.sql.transform.behaviors.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

/*
 * Applies when we can pick out dist key values.
 */
public class DistributionKeyExecuteTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.DISTRIBUTION_KEY_EXECUTE;
	}
	
	private boolean applies(SchemaContext sc, DMLStatement sp) throws PEException {
		if (sp.isDegenerate(sc) || sp.requiresRedistribution(sc))
			return false;
		// first make sure that any of the tables behind this statement actually has keys - this ought to be quick
		// do not traverse nested queries here - either it shouldn't matter or we should have handled it elsewhere		
		ListSet<TableKey> tables = EngineConstant.TABLES.getValue(sp,sc);
		if (tables == null || tables.isEmpty()) return false;
		boolean any = false;
		for(TableKey tk : tables) {
			if (tk.getAbstractTable().getDistributionVector(sc).usesColumns(sc)) { 
				any = true;
				break;
			}
		}
		if (!any) return false;
		// the degenerate test ensures that everything is already colocated - so if this is a container context
		// then we automagically apply by virtue of being a container/tenant.  note we are relying on the tenant dist
		// to be range dist on tenant id here
		if (sc.getPolicyContext().isAlwaysDistKey()) {
			return true;
		}
		// make a copy now in case we don't apply
		DMLStatement copy = CopyVisitor.copy(sp);
		DistKeyCollector dkc = new DistKeyCollector(sc,EngineConstant.WHERECLAUSE.getEdge(copy));
		Collection<TransformKeySeries> series = dkc.getSeries();
		Collection<TransformKeySimple> singles = dkc.getSingles();
		if (series.isEmpty() && singles.isEmpty()) return false;
		if (!series.isEmpty() && !singles.isEmpty()) return false; // too complex for us
		if (series.size() > 1 || singles.size() > 1) return false; // similarly too complex for us
		// does not apply if we have parameters - because we cannot sort
		if (sc.getValueManager().getNumberOfParameters() > 0) {
			for(TransformKeySeries tks : series) {
				if (tks.hasParameterizedValues())
					return false;
			}
		}
		sp.getBlock().store(DistributionKeyExecuteTransformFactory.class, new Pair<DMLStatement, DistKeyCollector>(copy,dkc));
		return true;
	}

	private static MappingSolution mapKey(SchemaContext sc, DMLStatement stmt, DistributionKey dk) throws PEException {
		return sc.getCatalog().mapKey(sc,dk.getDetachedKey(sc), dk.getModel(sc), stmt.getKeyOpType(), stmt.getSingleGroup(sc));
	}

	private static class MappedKeyEntry {
		
		private TransformKeySimple tk;
		private DistributionKey dk;
		private MappingSolution ms;
		
		public MappedKeyEntry(SchemaContext sc, DMLStatement sp, TransformKeySimple tks) throws PEException {
			tk = tks;
			dk = tk.buildKeyValue(sc);
			ms = mapKey(sc, sp, dk);
		}
		
		@SuppressWarnings("unused")
		public TransformKeySimple getOriginalKey() {
			return tk;
		}

		public DistributionKey getDistributionKey() {
			return dk;
		}
		
		public MappingSolution getMappingSolution() {
			return ms;
		}
	}

	@Override
	public FeatureStep plan(final DMLStatement orig, PlannerContext ipc)
			throws PEException {
		if (!applies(ipc.getContext(),orig))
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		
		@SuppressWarnings("unchecked")
		Pair<DMLStatement,DistKeyCollector> stmtAndKeyCollector = 
				(Pair<DMLStatement,DistKeyCollector>)orig.getBlock().getFromStorage(DistributionKeyExecuteTransformFactory.class);
		orig.getBlock().clearFromStorage(DistributionKeyExecuteTransformFactory.class);
		DistKeyCollector collector = null;
		DMLStatement actualStatement = orig;
		if (stmtAndKeyCollector != null) {
			collector = stmtAndKeyCollector.getSecond();
			actualStatement = stmtAndKeyCollector.getFirst();
		}
		Boolean cacheable = null;
		
		FeatureStep root = null;
		ExecutionCost cost = null;
		
		if (collector== null) {
			// container dist - find a table that is so distributed - that will be the key
			TableKey candidate = null;
			for(TableKey tk : EngineConstant.TABLES.getValue(actualStatement, context.getContext())) {
				if (tk.getAbstractTable().getDistributionVector(context.getContext()).isContainer()) {
					candidate = tk;
					break;
				}
			}
			ListOfPairs<PEColumn,ConstantExpression> values = new ListOfPairs<PEColumn,ConstantExpression>();
			values.add(candidate.getAbstractTable().getTenantColumn(context.getContext()), 
					context.getContext().getPolicyContext().getTenantIDLiteral(true));
			DistributionKey dk = candidate.getAbstractTable().getDistributionVector(context.getContext()).buildDistKey(context.getContext(), candidate,values);
			boolean wc = EngineConstant.WHERECLAUSE.has(actualStatement);
			cacheable = true;
			if (context.isCosting() && actualStatement instanceof SelectStatement)
				cost = Costing.buildBasicCost(context.getContext(), (SelectStatement)actualStatement, true);
			if (cost == null)
				cost = new ExecutionCost(wc,true,null,-1);
			DMLExplainRecord record = DMLExplainReason.TENANT_DISTRIBUTION.makeRecord();
			if (cost.getRowCount() > -1)
				record = record.withRowEstimate(cost.getRowCount());
			if (actualStatement instanceof ProjectingStatement) {
				ProjectingStatement ps = (ProjectingStatement) actualStatement;
				root = DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(context,
						this,
						ps, 
						cost,
						ps.getSingleGroup(context.getContext()),
						ps.getDatabase(context.getContext()),
						null,
						dk,
						record);
			} else {
				root = DefaultFeatureStepBuilder.INSTANCE.buildStep(context, this, actualStatement, dk, record);
			}
		} else {
			TransformKey tk = null;
			if (!collector.getSeries().isEmpty()) tk = collector.getSeries().iterator().next();
			if (!collector.getSingles().isEmpty()) tk = collector.getSingles().iterator().next();
			if (tk instanceof TransformKeySimple) {
				DistributionKey dk = ((TransformKeySimple)tk).buildKeyValue(context.getContext());
				cacheable = true;
				if (context.isCosting() && actualStatement instanceof SelectStatement) {
					cost = Costing.buildBasicCost(context.getContext(), (SelectStatement)actualStatement, true);
				} else {
					PlanningConstraint best = ConstraintCollector.findBestConstraint(context.getContext(), actualStatement, false);
					cost = new ExecutionCost(true,true,best,(best != null ? best.getRowCount() : -1));
				}
				if (actualStatement instanceof ProjectingStatement) {
					ProjectingStatement ps = (ProjectingStatement) actualStatement;
					root = DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(context,
							this,
							ps, 
							cost,
							ps.getSingleGroup(context.getContext()),
							ps.getDatabase(context.getContext()),
							dk.getTable().getDistributionVector(context.getContext()),
							dk,
							DMLStatement.distKeyExplain);
				} else {
					root = DefaultFeatureStepBuilder.INSTANCE.buildStep(context, this, actualStatement, dk, DMLStatement.distKeyExplain);
				}

			} else {
				cacheable = false;
				MultiMap<MappingSolution, MappedKeyEntry> bySite = new MultiMap<MappingSolution, MappedKeyEntry>();
				for(TransformKeySimple itk : ((TransformKeySeries)tk).getKeySource()) {
					MappedKeyEntry mke = new MappedKeyEntry(context.getContext(),actualStatement,itk);
					bySite.put(mke.getMappingSolution(),mke);
				}
				// todo: we don't always need to cost but some xform tests will fail if we don't, revisit this
				if (context.isCosting() && actualStatement instanceof SelectStatement)
					cost = Costing.buildBasicCost(context.getContext(), (SelectStatement)actualStatement, (bySite.keySet().size() == 1));
				else { 
					PlanningConstraint best = ConstraintCollector.findBestConstraint(context.getContext(), actualStatement, false);
					cost = new ExecutionCost(true,bySite.keySet().size() == 1, best,(best != null ? best.getRowCount() : -1));
				}
				
				DistributionKey dk = null;
				if (bySite.keySet().size() == 1) {
					// still single site - just multiple keys.  get one of them now.
					dk = bySite.values().iterator().next().getDistributionKey();
					DMLExplainRecord record = DMLStatement.distKeyExplain;
					if (cost.getRowCount() > -1)
						record = record.withRowEstimate(cost.getRowCount());
					if (actualStatement instanceof ProjectingStatement) {
						ProjectingStatement ps = (ProjectingStatement) actualStatement;
						root = DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(context,
								this,
								ps, 
								cost,
								ps.getSingleGroup(context.getContext()),
								ps.getDatabase(context.getContext()),
								dk.getTable().getDistributionVector(context.getContext()),
								dk,
								DMLStatement.distKeyExplain);
					} else {
						root = DefaultFeatureStepBuilder.INSTANCE.buildStep(context, this, actualStatement, dk, DMLStatement.distKeyExplain);
					}
				}
			}
		}
		if (root == null) {
			// todo: should plan the original instead of the modified stmt
			PEStorageGroup group = actualStatement.getSingleGroup(context.getContext());
			DistributionVector dv = EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(actualStatement,context.getContext());
			DMLExplainRecord rec = (cost.getRowCount() > -1 ?
											new DMLExplainRecord(DMLExplainReason.BASIC_PARTITION_QUERY,null,cost.getRowCount())
										: null);
			if (actualStatement instanceof ProjectingStatement) {
				root = DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(
					context, 
					this,
					(ProjectingStatement)actualStatement, 
					cost, 
					group, 
					(PEDatabase)actualStatement.getDatabase(context.getContext()),
					dv,
					null,
					rec);
			} else {
				root = DefaultFeatureStepBuilder.INSTANCE.buildStep(
						context, 
						this, 
						actualStatement, 
						null,
						rec);
			}
		}
		return root.withCachingFlag(cacheable);
	}
	
	
		
}
