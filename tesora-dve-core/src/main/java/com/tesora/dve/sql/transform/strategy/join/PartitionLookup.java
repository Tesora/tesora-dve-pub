// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.CollapsedJoinGraph;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.jg.DGMultiJoin;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.jg.JoinEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

class PartitionLookup {
	
	Map<DPart, OriginalPartitionEntry> entryByPartition = new HashMap<DPart, OriginalPartitionEntry>();
	Map<TableKey, DPart> partitionByTable = new HashMap<TableKey,DPart>();
	ListSet<OriginalPartitionEntry> allEntries = new ListSet<OriginalPartitionEntry>();
	RestrictionManager rm;
	MultiMap<ColumnKey,ColumnKey> restrictionForwarding;
	SelectStatement input;
	CollapsedJoinGraph jg;
	final SchemaContext sc;
	
	public PartitionLookup(SchemaContext sc, SelectStatement allParts) {
		input = allParts;
		this.sc = sc;
		jg = EngineConstant.PARTITIONS.getValue(input, sc);
		for(DPart dp : jg.getPartitions()) {
			for(TableKey tk : dp.getTables()) {
				partitionByTable.put(tk, dp);
			}
			OriginalPartitionEntry pe = new OriginalPartitionEntry(sc, input, dp);
			entryByPartition.put(dp, pe);
			allEntries.add(pe);
		}
		rm = new RestrictionManager(jg);
	}
	
	public RestrictionManager getRestrictionManager() {
		return rm;
	}
	
	public DPart getPartitionFor(TableKey dep) {
		DPart p = partitionByTable.get(dep);
		return p;
	}
	
	public ListSet<DPart> getPartitionsFor(ListSet<TableKey> deps) {
		ListSet<DPart> parts = new ListSet<DPart>();
		for(TableKey tk : deps) {
			parts.add(partitionByTable.get(tk));
		}
		return parts;
	}

	public void adapt(P3ProjectionBuffer proj, OriginalWhereBuffer wc, ExplicitJoinBuffer ejb) throws PEException {
		ListSet<BufferEntry> usedProjEntries = new ListSet<BufferEntry>();
		ListSet<BufferEntry> usedWCEntries = new ListSet<BufferEntry>();
		ListSet<JoinBufferEntry> usedJoins = new ListSet<JoinBufferEntry>();
		for(OriginalPartitionEntry pe : entryByPartition.values()) {
			DPart part = pe.getPartition();
			ListSet<BufferEntry> partProj = proj.getEntriesFor(part);
			ListSet<BufferEntry> partFilter = wc.getEntriesFor(part);
			ListSet<JoinBufferEntry> partJoins = ejb.getJoinEntriesFor(part);
			usedProjEntries.addAll(partProj);
			usedWCEntries.addAll(partFilter);
			usedJoins.addAll(partJoins);
			pe.adapt(input, partProj, partFilter, partJoins, rm.findPropagatedRestrictions(part), rm.findNonPropagatingRestrictions(part));
		}
		ListSet<BufferEntry> bridgingProjection = new ListSet<BufferEntry>(proj.getEntries());
		ListSet<BufferEntry> bridgingFilter = new ListSet<BufferEntry>(wc.getEntries());
		ListSet<BufferEntry> bridgingJoins = new ListSet<BufferEntry>(ejb.getEntries());
		bridgingProjection.removeAll(usedProjEntries);
		bridgingFilter.removeAll(usedWCEntries);
		bridgingJoins.removeAll(usedJoins);
		proj.setBridging(bridgingProjection);
		wc.setBridging(bridgingFilter);
		ejb.setBridging(bridgingJoins);
	}
	
	public Collection<OriginalPartitionEntry> getPartitionEntries() {
		return entryByPartition.values();
	}
	
	public ListSet<JoinEntry> buildNonColocatedJoins(JoinRewriteAdaptedTransform pt, JoinRewriteTransformFactory factory) throws PEException {
		ListSet<BinaryJoinEntry> out = new ListSet<BinaryJoinEntry>();
		List<JoinEdge> invalidJoins = jg.getJoins();
		// keep track of all used partitions
		ListSet<OriginalPartitionEntry> used = new ListSet<OriginalPartitionEntry>();
		MultiMap<OriginalPartitionEntry,RegularJoinEntry> clustered = new MultiMap<OriginalPartitionEntry,RegularJoinEntry>();
		LinkedHashMap<DGJoin,MultiJoinEntry> multiJoins = new LinkedHashMap<DGJoin,MultiJoinEntry>();
		for(JoinEdge je : invalidJoins) {
			OriginalPartitionEntry lp = entryByPartition.get(getPartitionFor(je.getLHSTab()));
			OriginalPartitionEntry rp = entryByPartition.get(getPartitionFor(je.getRHSTab()));
			used.add(rp);
			if (je.getJoin().isMultijoin()) {
				MultiJoinEntry ex = multiJoins.get(je.getJoin());
				if (ex != null) continue;
				// all the lhs parts, and the rhs part
				ListSet<OriginalPartitionEntry> lhs = new ListSet<OriginalPartitionEntry>();
				for(JoinEdge sj : je.getJoin().getEdges()) {
					OriginalPartitionEntry mjlp = entryByPartition.get(getPartitionFor(sj.getLHSTab())); 
					lhs.add(mjlp);
					used.add(mjlp);
				}
				MultiJoinEntry mje = new MultiJoinEntry(sc, input, lhs, (DGMultiJoin) je.getJoin(), rp, pt, factory);
				multiJoins.put(je.getJoin(),mje);
			} else {			
				RegularJoinEntry rje = new RegularJoinEntry(sc, input, je.getJoin(), lp, rp, pt, factory);
				clustered.put(rje.getLeftPartition(), rje);
				clustered.put(rje.getRightPartition(), rje);
				out.add(rje);
				used.add(lp);
			}
		}
		// now, if there are any unused partitions - schedule them as a cartesian join
		ListSet<OriginalPartitionEntry> copy = new ListSet<OriginalPartitionEntry>(allEntries);
		copy.removeAll(used);
		
		if (copy.size() > 1) {
			OriginalPartitionEntry alreadyUsed = null;
			if (out.size() > 1)
				alreadyUsed = out.get(0).getLeftPartition();
			List<OriginalPartitionEntry> scoredRemaining = FinalBuffer.buildScoredList(copy, new UnaryFunction<ExecutionCost, OriginalPartitionEntry>() {

				@Override
				public ExecutionCost evaluate(OriginalPartitionEntry object) {
					try {
						return object.getScore();
					} catch (PEException pe) {
						throw new SchemaException(Pass.PLANNER, "Score not available", pe);
					}
				}
				
			});
			for(int i = 0; i < scoredRemaining.size(); i++) {
				OriginalPartitionEntry leftEntry = alreadyUsed;
				OriginalPartitionEntry rightEntry = scoredRemaining.get(i);
				alreadyUsed = rightEntry;
				if (leftEntry == null)
					continue;
				out.add(new CartesianJoinEntry(sc, input,leftEntry,rightEntry,pt, factory));
			}			
		}
		// finally, let's see if any of our joins are actually clustered.  in that case, use a clustered join entry
		ListSet<JoinEntry> actual = null;
		// first, get rid of any part of the clustered map that only has one join entry
		for(OriginalPartitionEntry pe : used) {
			Collection<RegularJoinEntry> sub = clustered.get(pe);
			if (sub == null || sub.isEmpty() || sub.size() == 1) clustered.remove(pe);
		}
		if (!clustered.isEmpty()) {
			actual = buildClustered(clustered,out,pt.getBuffers(),pt,factory);
		} else {
			actual = new ListSet<JoinEntry>();
			for(BinaryJoinEntry bje : out)
				actual.add(bje);
		}

		actual.addAll(multiJoins.values());
		return actual;
	}
	
	private ListSet<JoinEntry> buildClustered(MultiMap<OriginalPartitionEntry,RegularJoinEntry> clustered,
				ListSet<BinaryJoinEntry> current,
				RewriteBuffers buffers,
				JoinRewriteAdaptedTransform pt,
				JoinRewriteTransformFactory factory) throws PEException {
		ListSet<JoinEntry> out = new ListSet<JoinEntry>();
		for(OriginalPartitionEntry pe : clustered.keySet()) {
			List<RegularJoinEntry> sub = (List<RegularJoinEntry>) clustered.get(pe);
			if (sub.size() != current.size()) continue;
			// sort by join columns
			// organize the RJEs by join columns to the base; analyze each in turn.
			MultiMap<List<ColumnKey>, RegularJoinEntry> byJoinColumns = new MultiMap<List<ColumnKey>, RegularJoinEntry>();
			for(RegularJoinEntry rje : sub) {
				List<ColumnKey> jck = getBaseJoinColumns(pe,rje);
				byJoinColumns.put(jck,rje);
			}
			for(List<ColumnKey> jck : byJoinColumns.keySet()) {
				List<RegularJoinEntry> coljoins = (List<RegularJoinEntry>) byJoinColumns.get(jck);
				if (coljoins.size() < 2) 
					// schedule as a regular join
					continue;
				List<ClusteredJoinEntry> cjes = buildClustered(pe,coljoins,buffers,pt,factory,jck);
				for(ClusteredJoinEntry cje : cjes) {
					for(RegularJoinEntry rje : cje.getComponentParts())
						current.remove(rje);
					out.add(cje);
				}
			}
		}
		for(BinaryJoinEntry bje : current)
			out.add(bje);
		return out;
	}
			
	private List<ClusteredJoinEntry> buildClustered(OriginalPartitionEntry basePartition, List<RegularJoinEntry> joins, 
			RewriteBuffers buffers, JoinRewriteAdaptedTransform pt, JoinRewriteTransformFactory factory, List<ColumnKey> unionBaseColumns) throws PEException {
				
		ListSet<RegularJoinEntry> innerJoins = new ListSet<RegularJoinEntry>();
		ListSet<RegularJoinEntry> outerJoins = new ListSet<RegularJoinEntry>();
		boolean clustered = true; // assume
		
		// keep track of the joined tables we see in all of the entries
		// we need to figure out what the actual join order will be
		HashMap<JoinedTable, RegularJoinEntry> jts = new HashMap<JoinedTable,RegularJoinEntry>();
		ListSet<RegularJoinEntry> informal = new ListSet<RegularJoinEntry>();
		
		String prefix = null;
		if (pt.emitting())
			prefix = "Unable to build ucj for " + basePartition + " because ";

		// also verify that we have a single other group
		PEStorageGroup otherGroup = null;
		
		for(RegularJoinEntry rje : joins) {
			DGJoin dgj = rje.getJoin();
			if (dgj.getJoin() == null)
				// informal wc join
				innerJoins.add(rje);
			 else if (dgj.getJoinType().isOuterJoin())
				outerJoins.add(rje);
			else
				innerJoins.add(rje);
			if (clustered) {
				if (!dgj.isSimpleJoin()) {
					clustered = false;
					if (pt.emitting()) 
						pt.emit(prefix + dgj.getJoin() + " is not simple");
					continue;
				}
			}
			if (dgj.getJoin() != null)
				jts.put(dgj.getJoin(), rje);
			else
				informal.add(rje);
			// also put in everything within the partition
			for(JoinBufferEntry jbe : rje.getLeftPartition().getExplicitJoins())
				jts.put(jbe.getJoinedTable(), rje);
			for(JoinBufferEntry jbe : rje.getRightPartition().getExplicitJoins())
				jts.put(jbe.getJoinedTable(), rje);	
			OriginalPartitionEntry op = rje.getOtherPartition(basePartition);
			if (otherGroup == null)
				otherGroup = op.getSourceGroup();
			else if (!op.getSourceGroup().isSubsetOf(sc, otherGroup)) {
				clustered = false;
				if (pt.emitting())
					pt.emit(prefix + dgj.getJoin() + " storage group is " + op.getSourceGroup() + " but needs to be " + otherGroup);
			}
		}
		MultiMap<FromTableReference,JoinedTable> logicalOrder = buffers.getJoinsBuffer().getLogicalOrder();
		ListSet<FromTableReference> baseTables = new ListSet<FromTableReference>();
		for(JoinedTable jt : jts.keySet())
			baseTables.add(jt.getEnclosingFromTableReference());
		ListSet<RegularJoinEntry> actualOrder = new ListSet<RegularJoinEntry>();
		for(FromTableReference ftr : logicalOrder.keySet()) {
			if (!baseTables.contains(ftr)) continue;
			List<JoinedTable> sub = (List<JoinedTable>) logicalOrder.get(ftr);
			if (sub == null || sub.isEmpty()) continue;
			for(JoinedTable jt : sub) {
				RegularJoinEntry mrje = jts.get(jt);
				if (mrje != null)
					actualOrder.add(mrje);
			}
		}
		List<ClusteredJoinEntry> out = new ArrayList<ClusteredJoinEntry>();
		if (actualOrder.size() != joins.size()) {
			// doesn't work somehow, see if it's because we have informal joins
			if ((actualOrder.size() + informal.size()) == joins.size()) {
				// still works, but whether the informal entries are evaluated first or last is still unknown
			} else {
				if (pt.emitting()) {
					pt.emit("Failed to account for all joins for base columns " + unionBaseColumns);
					pt.emit("All joins: " + Functional.joinToString(joins, PEConstants.LINE_SEPARATOR));
					pt.emit("Computed explicit join order: " + Functional.joinToString(actualOrder, PEConstants.LINE_SEPARATOR));
					pt.emit("Informal joins: " + Functional.joinToString(informal, PEConstants.LINE_SEPARATOR));
				}
				return out;
			}
		}
		// if we get here we think we can process the cluster.  we know what the common join condition is.  
		if (clustered) {
			if ((!outerJoins.isEmpty() && basePartition.getScore().getConstraint() == null && !basePartition.getScore().hasWhereClause()) ||
					partitionsHaveTempTables())
				return out;
			out.add(new ClusteredJoinEntry(sc, basePartition.getBasis(),
					pt,factory,
					basePartition,actualOrder,informal,otherGroup,unionBaseColumns));
		}
		return out;
	}

	private List<ColumnKey> getBaseJoinColumns(OriginalPartitionEntry basePartition, RegularJoinEntry rje) {
		DGJoin dgj = rje.getJoin();
		List<ExpressionNode> jc = null;
		if (basePartition.getPartition().getTables().contains(dgj.getLeftTable()))
			jc = dgj.getRedistJoinExpressions(dgj.getLeftTable());
		else
			jc = dgj.getRedistJoinExpressions(dgj.getRightTable());
		List<ColumnKey> jck = Functional.apply(jc, new UnaryFunction<ColumnKey,ExpressionNode>() {

			@Override
			public ColumnKey evaluate(ExpressionNode object) {
				ColumnInstance ci = (ColumnInstance) object;
				return ci.getColumnKey();
			}

		});
		if (jck.size() > 1)
			Collections.sort(jck, new Comparator<ColumnKey>() {

				@Override
				public int compare(ColumnKey o1, ColumnKey o2) {
					int p1 = o1.getPEColumn().getPosition();
					int p2 = o2.getPEColumn().getPosition();
					if (p1 < p2)
						return -1;
					if (p1 > p2)
						return 1;
					return 0;
				}

			});
		return jck;
	}

	private boolean partitionsHaveTempTables() {
		boolean ret = false;
		for(TableKey tk : partitionByTable.keySet()) {
			if (tk.getAbstractTable().isTempTable()) {
				return true;
			}
		}
		return ret;
	}
	
}