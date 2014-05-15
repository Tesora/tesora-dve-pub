// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;
import com.tesora.dve.sql.util.ListSet;

public abstract class JoinEntry {

	protected SelectStatement basis;
	protected JoinRewriteAdaptedTransform parentTransform;
	protected final JoinRewriteTransformFactory featurePlanner;
	protected ExecutionCost score;
	
	protected ListSet<JoinEntry> references;

	protected final SchemaContext sc;
	protected final PlannerContext pc;
	protected final long id;
	
	public JoinEntry(PlannerContext pc, SchemaContext sc, SelectStatement basis, JoinRewriteAdaptedTransform parent, JoinRewriteTransformFactory factory) {
		this.basis = basis;
		this.parentTransform = parent;
		references = new ListSet<JoinEntry>();
		this.sc = sc;
		this.pc = pc;
		this.id = sc.getNextObjectID();
		this.featurePlanner = factory;
	}
	
	public SchemaContext getSchemaContext() {
		return sc;
	}
	
	public PlannerContext getPlannerContext() {
		return pc;
	}
	
	public SelectStatement getBasis() {
		return basis;
	}
	
	public JoinRewriteAdaptedTransform getParentTransform() {
		return parentTransform;
	}

	public JoinRewriteTransformFactory getFeaturePlanner() {
		return featurePlanner;
	}
	
	public ExecutionCost getScore() throws PEException{
		if (score == null)
			score = computeScore();
		return score;
	}
	
	protected abstract ExecutionCost computeScore() throws PEException;
	
	protected void clearScore() {
		score = null;
		// also clear on all my refs
		for(JoinEntry je : references)
			je.clearScore();
	}
	
	public abstract ListSet<DPart> getPartitions();
	
	// might be more than one for clustered joins
	public abstract DGJoin getJoin();
	
	public ListSet<DGJoin> getClusteredJoins() {
		return null;
	}
	
	public boolean isOuterJoin() {
		if (getJoin() == null) return false;
		return getJoin().getJoinType().isOuterJoin();
	}
		
	// schedule as an independent join
	public abstract JoinedPartitionEntry schedule() throws PEException; 
	
	public abstract JoinedPartitionEntry schedule(JoinedPartitionEntry head) throws PEException;
	
	public abstract JoinedPartitionEntry schedule(List<JoinedPartitionEntry> ipes) throws PEException;

	public boolean schedule(ListSet<JoinedPartitionEntry> heads) throws PEException {
		return findMatchingJoins(heads);
	}

	protected abstract boolean preferNewHead(ListSet<JoinedPartitionEntry> head) throws PEException;
	
	public boolean findMatchingJoins(ListSet<JoinedPartitionEntry> heads) throws PEException {
		if (!heads.isEmpty() && buildIntIntJoin(heads))
			return true;
		// we can prefer a new head iff none of our partitions are in an existing head
		// we however cannot prefer a new head if one of our partitions is part of the existing partitions
		boolean partiallyContained = false;
		ListSet<DPart> myParts = getPartitions();
		for(JoinedPartitionEntry ipe : heads) {
			for(DPart dp : myParts) {
				if (ipe.getPartitions().contains(dp)) {
					partiallyContained = true;
					break;					
				}
			}
		}
		boolean trynew = preferNewHead(heads);
		if (trynew && !partiallyContained) {
			if (tryNewHead(heads))
				return true;
		}
		if (!heads.isEmpty() && buildIndIntJoin(heads))
			return true;
		if (!trynew && !partiallyContained) {
			if (tryNewHead(heads))
				return true;
		}
		return false;
	}

	protected boolean tryNewHead(ListSet<JoinedPartitionEntry> heads) throws PEException {
		if (parentTransform.canschedule(this)) {
			if (parentTransform.emitting())
				parentTransform.emit("tryNewHead(heads) on " + toString());
			JoinedPartitionEntry ipe = schedule();			
			heads.add(ipe);
			if (parentTransform.emitting()) {
				parentTransform.emit("Created initial ipe: ");
				parentTransform.emit(ipe.toString());
				parentTransform.emit("using " + this);
			}
			return true;				
		}
		return false;
	}
	
	public boolean buildIntIntJoin(ListSet<JoinedPartitionEntry> heads) throws PEException {
		// essentially, we pass here if our join can work on the existing heads
		final Map<DPart, JoinedPartitionEntry> ipeForPartition = new HashMap<DPart, JoinedPartitionEntry>();
		for(JoinedPartitionEntry ipe : heads) {
			for(DPart p : ipe.getPartitions())
				ipeForPartition.put(p, ipe);
		}
		List<JoinedPartitionEntry> ipes = new ListSet<JoinedPartitionEntry>();
		for(DPart p : getPartitions()) {
			JoinedPartitionEntry ipe = ipeForPartition.get(p);
			if (ipe == null) return false;
			ipes.add(ipe);
		}
		if (!parentTransform.canschedule(this))
			return false;
		if (parentTransform.emitting())
			parentTransform.emit("buildIntIntJoin(heads) on " + toString());

		JoinedPartitionEntry newIpe = schedule(ipes);
		for(JoinedPartitionEntry ipe : ipes)
			heads.remove(ipe);
		heads.add(newIpe);
		if (parentTransform.emitting()) {
			parentTransform.emit("created ipe from ipes " + newIpe);
			parentTransform.emit("using " + this);
		}
		return true;
	}

	public boolean buildIndIntJoin(ListSet<JoinedPartitionEntry> heads) throws PEException {
		HashSet<DPart> usedPartitions = new HashSet<DPart>();
		for(JoinedPartitionEntry ipe : heads)
			usedPartitions.addAll(ipe.getPartitions());
		ListSet<DPart> currentUsed = new ListSet<DPart>();
		ListSet<DPart> currentUnused = new ListSet<DPart>();
		for(DPart p : getPartitions()) {
			if (usedPartitions.contains(p))
				currentUsed.add(p);
			else
				currentUnused.add(p);
		}
		if (!currentUsed.isEmpty() && !currentUnused.isEmpty()) {
			// join a new table in to an existing intermediate partition.  figure out which one.
			if (currentUsed.size() > 1)
				throw new PEException("Unable to schedule int int join with multiple used partitions");
			DPart usedPartition = currentUsed.get(0); 
			for(JoinedPartitionEntry ipe : heads) {
				if (ipe.getPartitions().contains(usedPartition) && parentTransform.canschedule(this)) {
					if (parentTransform.emitting())
						parentTransform.emit("buildIndIntJoin(heads) on " + toString());

					JoinedPartitionEntry newHead = null;
					if (parentTransform.getPlannerContext() != null)
						newHead = schedule(ipe);
					else
						newHead = schedule(ipe);
					heads.remove(ipe);
					heads.add(newHead);
					if (parentTransform.emitting()) {
						parentTransform.emit("created ipe from ipe and pe " + newHead);
						parentTransform.emit("using " + this);
					}
					return true;
				}
			}
		}
		return false;
	}

	public void addReference(JoinEntry other) {
		references.add(other);
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "@" + id;
	}
}
