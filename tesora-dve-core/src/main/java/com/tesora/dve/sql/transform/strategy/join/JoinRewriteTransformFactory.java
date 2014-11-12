package com.tesora.dve.sql.transform.strategy.join;

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


import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.jg.JoinGraph;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.StrategyHint;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * Applies if there are multiple partitions.
 */
public class JoinRewriteTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.JOIN;
	}

	static class JoinRewriteAdaptedTransform {

		protected PartitionLookup originalPartitions;
		protected SelectStatement allParts;
		protected RewriteBuffers buffers;
		protected JoinOrdering ordering;

		protected final PlannerContext plannerContext;
		
		// joins whose partitions are not colocated - what we're planning
		protected ListSet<JoinEntry> nonColocatedJoins = null;
		
		protected JoinedPartitionEntry execRoot = null;
		
		protected final JoinRewriteTransformFactory factory;

		public JoinRewriteAdaptedTransform(PlannerContext pc,
				JoinRewriteTransformFactory factory,
				SchemaContext sc, 
				DMLStatement s, 
				StrategyHint hint,
				SelectStatement ss,
				PartitionLookup origPartitions,
				RewriteBuffers sortingBuffers,
				JoinOrdering ordering) {
			originalPartitions = origPartitions;
			allParts = ss;
			buffers = sortingBuffers;
			this.ordering = ordering;
			this.plannerContext = pc;
			this.factory = factory;
		}

		public void emit(String what) {
			factory.emit(what);
		}

		public boolean emitting() {
			return factory.emitting();
		}

		
		public PlannerContext getPlannerContext() {
			return plannerContext;
		}
		
		public SchemaContext getSchemaContext() {
			return getPlannerContext().getContext();
		}
		
		public RewriteBuffers getBuffers() {
			return buffers;
		}
		
		public boolean canschedule(JoinEntry je) {
			return ordering.canschedule(getSchemaContext(), je);
		}
		
		private JoinedPartitionEntry buildExecutionOrder() throws PEException {
			nonColocatedJoins = originalPartitions.buildNonColocatedJoins(this,factory);
			if (factory.emitting()) {
				for(OriginalPartitionEntry pe : originalPartitions.getPartitionEntries())
					factory.emit(pe.getPostPlanningState());
				buffers.describeState(System.out);
				ordering.describe(getSchemaContext(), System.out);
				originalPartitions.getRestrictionManager().describe(System.out);
			}
			
			ListSet<JoinedPartitionEntry> heads = new ListSet<JoinedPartitionEntry>();
			
			boolean done = false;
			while(!done) {
				// recall that p2 explodes all proj entries, therefore all partition projections only contain columns - so we
				// no longer try to find matching projections
				boolean anythingChanged = buffers.apply(heads, nonColocatedJoins.isEmpty());
				if (!anythingChanged) {
					ListSet<JoinEntry> scored = score();
					if (factory.emitting()) {
						factory.emit("Scored:");
						for(JoinEntry je : scored)
							factory.emit("  " + je);
						factory.emit("Heads:");
						for(JoinedPartitionEntry ipe : heads)
							factory.emit("  " + ipe);
					}
					for(JoinEntry je : scored) {
						anythingChanged = je.schedule(heads);
						if (anythingChanged) {
							nonColocatedJoins.remove(je);
							break;
						}
					}					
					if (!anythingChanged && !nonColocatedJoins.isEmpty()) {
						// debugging
						// buildInitialJoins(heads);
						throw new PEException("Cannot schedule initial set of joins");
					}

				}
				done = !anythingChanged;
			}
			// it's an error if anything is left in the scores
			if (!nonColocatedJoins.isEmpty()) 
				throw new PEException("Finished planning join but have remaining unscored joins: "
						+ Functional.joinToString(nonColocatedJoins, ", "));
			if (!buffers.getWhereClauseBuffer().getScoredBridging().isEmpty()) {
				throw new PEException("Finished planning join, but have remaining scored filters: "
						+ Functional.joinToString(buffers.getWhereClauseBuffer().getScoredBridging(), ", "));
			}
			
			// there should be exactly one head in heads; that's our final result.
			if (heads.size() != 1) 
				throw new PEException("Finished planning join, but have more than one final query");
			
			return heads.get(0);			
		}

		private ListSet<JoinEntry> score() throws PEException {
			return score(nonColocatedJoins);
		}
		
		public static ListSet<JoinEntry> score(ListSet<JoinEntry> in) throws PEException {
			return FinalBuffer.buildScoredList(in, new UnaryFunction<ExecutionCost, JoinEntry>() {
				@Override
				public ExecutionCost evaluate(JoinEntry object) {
					try {
						return object.getScore();
					} catch (PEException pe) {
						throw new SchemaException(Pass.PLANNER, "score not available");
					}
				}								
			});
		}
		
		
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!(stmt instanceof SelectStatement))
			return null;
		if (stmt.getDerivedInfo().getLocalTableKeys().isEmpty())
			return null;
		// have to consider the single storage site rewrite override
		JoinGraph jg = EngineConstant.PARTITIONS.getValue(stmt,ipc.getContext());
		if (!jg.requiresRedistribution())
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		SelectStatement orig = (SelectStatement) stmt;
		SelectStatement allParts = CopyVisitor.copy(orig);

		PartitionLookup partitions = new PartitionLookup(context.getContext(), allParts);
		JoinOrdering ordering = new JoinOrdering(EngineConstant.PARTITIONS.getValue(allParts, context.getContext()));
		RewriteBuffers buffers = new RewriteBuffers(partitions, context.getContext());
		buffers.adapt(allParts);
		
		PlannerContext childContext = context.withCosting();
				
		JoinRewriteAdaptedTransform jrat = new JoinRewriteAdaptedTransform(childContext, this,
				context.getContext(), stmt,null,allParts,partitions, buffers, ordering);
		
		for(OriginalPartitionEntry pe : partitions.getPartitionEntries()) {
			pe.setParentTransform(jrat);
		}
		
		if (emitting())
			emit(allParts.getSQL(context.getContext(),EmitOptions.NONE.addMultilinePretty("  "),true));
		
		for(OriginalPartitionEntry pe : partitions.getPartitionEntries()) {
			SelectStatement partitionQuery = pe.getChildCopy();
			if (emitting())
				emit(pe.getPrePlanningState());
			pe.setStep((ProjectingFeatureStep) buildPlan(partitionQuery,childContext, DefaultFeaturePlannerFilter.INSTANCE));
		}

		FeatureStep fs= jrat.buildExecutionOrder().getStep(buffers);
		return fs;
	}
	
}
