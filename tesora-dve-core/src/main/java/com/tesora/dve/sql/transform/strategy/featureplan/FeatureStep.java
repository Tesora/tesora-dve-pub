// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.AbstractTraversal.ExecStyle;
import com.tesora.dve.sql.node.AbstractTraversal.Order;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ParallelExecutionStep;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryPredicate;

public abstract class FeatureStep {

	// children we might have
	protected final List<FeatureStep> selfChildren;
	// scheduling prefix; we need a separate list for cases where partially plan, rewrite, then plan the result
	protected final List<FeatureStep> preChildren;
	// the generators for this step; usually just one but if we chained something more than one
	protected ListSet<FeaturePlanner> planners;
	
	protected DMLExplainRecord explain = null;
	
	protected Boolean cacheable = null;

	protected boolean maintainInvariants = true;
	
	// the group this step is on
	protected final PEStorageGroup group;
	
	protected DistributionKey distributionKey;

	protected boolean parallelChildren = false; // default is children are executed sequentially
	
	// sometimes self may not be executed, because we rewrote the query and joined it to something else
	protected boolean scheduleSelf = true; 
	
	public FeatureStep(FeaturePlanner planner, PEStorageGroup srcGroup, DistributionKey key) {
		this.selfChildren = new ArrayList<FeatureStep>();
		this.preChildren = new ArrayList<FeatureStep>();
		this.planners = new ListSet<FeaturePlanner>();
		if (planner == null)
			throw new SchemaException(Pass.PLANNER, "Must have planner for feature step");
		this.planners.add(planner);
		this.group = srcGroup;
		this.distributionKey = key;
	}
	
	public List<FeatureStep> getSelfChildren() {
		return selfChildren;
	}
	
	public List<FeatureStep> getPreChildren() {
		return preChildren;
	}

	public List<FeatureStep> getAllChildren() {
		ArrayList<FeatureStep> out = new ArrayList<FeatureStep>();
		out.addAll(preChildren);
		out.addAll(selfChildren);
		return out;
	}

	public void prefixChildren(List<FeatureStep> steps) {
		preChildren.addAll(steps);
	}
	
	public void addChild(FeatureStep fs) {
		selfChildren.add(fs);
	}
	
	public PEStorageGroup getSourceGroup() {
		return group;
	}
	
	public DMLExplainRecord getExplainRecord() {
		return explain;
	}
	
	public DistributionKey getDistributionKey() {
		return distributionKey;
	}
	
	public FeatureStep withExplain(DMLExplainRecord rec) {
		explain = rec;
		return this;
	}
	
	public FeatureStep withParallelChildren() {
		parallelChildren = true;
		return this;
	}
	
	public FeatureStep withCachingFlag(Boolean v) {
		if (cacheable == null)
			cacheable = v;
		return this;
	}
	
	public FeatureStep withDefangInvariants() {
		maintainInvariants = false;
		return this;
	}
	
	public FeatureStep withPlanner(FeaturePlanner fp) {
		planners.add(fp);
		return this;
	}
	
	public void omitSelf() {
		scheduleSelf = false;
	}
	
	public boolean hasPlanner(final FeaturePlannerIdentifier tid) {
		return Functional.any(planners, new UnaryPredicate<FeaturePlanner>() {

			@Override
			public boolean test(FeaturePlanner object) {
				return object.getFeaturePlannerID().equals(tid);
			}
			
		});
	}
	
	protected final void schedulePrefix(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
		// prefix children are always sequential
		for(FeatureStep fs : preChildren)
			fs.schedule(sc,es,scheduled);		
	}
	
	protected void scheduleChildren(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
		schedulePrefix(sc,es,scheduled);
		if (selfChildren.isEmpty()) return;
		if (parallelChildren) {
			ParallelExecutionStep pes = new ParallelExecutionStep(getDatabase(sc), getSourceGroup());
			for(FeatureStep fs : selfChildren) {
				ExecutionSequence subes = new ExecutionSequence(es.getPlan());
				fs.schedule(sc, subes, scheduled);
				pes.addSequence(subes);
			}
			es.append(pes);
		} else {
			for(FeatureStep fs : selfChildren) {
				fs.schedule(sc,es, scheduled);
			}
		}
	}
	
	protected abstract void scheduleSelf(PlannerContext sc, ExecutionSequence es) throws PEException;
	
	public void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
		if (!scheduled.add(this)) return;
		scheduleChildren(sc,es,scheduled);
		if (cacheable != null && es.getPlan() != null)
			es.getPlan().setCacheable(cacheable);
		if (scheduleSelf)
			scheduleSelf(sc,es);
	}
	
	public abstract DMLStatement getPlannedStatement();
	
	public abstract Database<?> getDatabase(PlannerContext pc);
	
	protected void maintainInvariants(SchemaContext sc) {
		if (!maintainInvariants) return;
		FeaturePlanner fp = planners.get(planners.size() - 1); // use the last one, since it would be outermost
		if (getPlannedStatement().requiresRedistribution(sc)) {
			if(fp.emitting()) {
				fp.emit("Still requires redistribution.  Join graph:");
				fp.emit(EngineConstant.PARTITIONS.getValue(getPlannedStatement(), sc).describe(sc));
				fp.emit(getPlannedStatement().getSQL(sc));
			}
			getPlannedStatement().getBlock().clear();
			getPlannedStatement().requiresRedistribution(sc);
			throw new SchemaException(Pass.PLANNER,"Feature planner " + fp + " failed, redistribution still required for stmt " + getPlannedStatement().getSQL(sc));
		}
		if (EngineConstant.TABLES_INC_NESTED.hasValue(getPlannedStatement(),sc) 
				&& !EngineConstant.DISTRIBUTION_VECTORS.hasValue(getPlannedStatement(),sc))
			throw new SchemaException(Pass.PLANNER,"Planning failed, unable to determine distribution info");
	}
	
	public boolean isDML() {
		return true;
	}
	
	public void maintainInvariants(final PlannerContext pc) {
		new StepTraversal(Order.POSTORDER, ExecStyle.ONCE) {

			@Override
			public FeatureStep action(FeatureStep in) {
				in.maintainInvariants(pc.getContext());
				return in;
			}
			
		}.traverse(this);
	}

	public void display(final PlannerContext pc, final List<String> lines) {
		new StepTraversal(Order.PREORDER, ExecStyle.ONCE) {

			@Override
			public FeatureStep action(FeatureStep in) {
				StringBuffer indent = new StringBuffer();
				for(int i = 0; i < getPath().size(); i++)
					indent.append("  ");
				indent.append(in.describe(pc));
				lines.add(indent.toString());
				return in;
			}
			
		}.traverse(this);
	}
	
	public void display(PlannerContext pc, PrintStream whereTo, String header) {
		whereTo.println(header);
		ArrayList<String> lines = new ArrayList<String>();
		display(pc,lines);
		for(String s : lines)
			whereTo.println(s);
	}
	
	public String describe(PlannerContext pc) {
		return getClass().getSimpleName() + ": '" + getPlannedStatement().getSQL(pc.getContext()) + "'";
	}
	
}
