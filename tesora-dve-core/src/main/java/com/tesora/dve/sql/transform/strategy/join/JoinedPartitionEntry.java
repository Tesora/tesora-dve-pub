// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;




import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * A JoinedPartitionEntry is the result of colocating two other partition entries and executing the join between them.
 * The other partition entries may be original or joined.
 */
class JoinedPartitionEntry extends PartitionEntry {

	protected List<PartitionEntry> entries;
	
	// raw join stmt before we take any filters, etc
	protected SelectStatement joinStatement;
	protected PEStorageGroup currentGroup;
	protected Set<DistributionVector> dists;
	
	protected UniquedExpressionList filters;
	protected ListSet<BufferEntry> rawFilters;
	protected List<BufferEntry> projectionEntries;
	
	protected MultiMap<TableInstance, JoinedTable> explicitJoins;
	
	// the final query, different from the kernel
	protected SelectStatement finalSelect;
	
	// delegate scheduling to the owning adapted transform
	protected JoinRewriteAdaptedTransform parentTransform;
	// we need the factory for the feature steps
	protected final JoinRewriteTransformFactory factory;
	
	// we need to know the original join that we're implementing - in particular for
	// outer joins we delay taking filters
	protected boolean outerJoin;
	
	// whether or not to schedule our children in parallel - not applicable sometimes
	protected boolean parallelChildren;
	
	protected ExecutionCost specifiedScore;
	
	protected final PlannerContext plannerContext;
	
	public JoinedPartitionEntry(PlannerContext pc, SchemaContext sc, SelectStatement orig, 
			SelectStatement js, 
			List<PartitionEntry> components,
			PEStorageGroup targetGroup, 
			Set<DistributionVector> computedDists,
			ExecutionCost computedScore,
			JoinRewriteAdaptedTransform parent,
			JoinRewriteTransformFactory factory,
			boolean isOuterJoin,
			boolean parallelChildren,
			DMLExplainRecord explainHelp) {
		super(sc, orig, explainHelp);
		joinStatement = js;
		outerJoin = isOuterJoin;
		entries = components;
		currentGroup = targetGroup;
		dists = computedDists;
		if (computedDists.isEmpty())
			throw new SchemaException(Pass.PLANNER, "Missing computed dist vects");
		this.parallelChildren = parallelChildren;
		// start from decomposed filters
		filters = new UniquedExpressionList();
		filters.addAll(ExpressionUtils.decomposeAndClause(js.getWhereClause()));
		explicitJoins = new MultiMap<TableInstance, JoinedTable>();
		projectionEntries = new ArrayList<BufferEntry>();
		rawFilters = new ListSet<BufferEntry>();
		for(PartitionEntry jre : entries)
			projectionEntries.addAll(jre.getBufferEntries());
		parentTransform = parent;
		this.factory = factory;
		specifiedScore = computedScore;
		for(PartitionEntry jre : components) {
			jre.addReferencingEntry(this);
			jre.setPlanned(this);
		}
		setExplain(explainHelp);
		plannerContext = pc;
		sane("construction",projectionEntries.size(),joinStatement.getProjectionEdge().size(), joinStatement);		
	}
	
	public boolean take(OriginalWhereBuffer owb, boolean last) {
		// figure out if we were involved in an outer join.  in an outer join we take the test anyways
		// but leave it for the final step as well.
		Set<DPart> usedPartitions = getPartitions();
		boolean any = false;
		for(Iterator<BufferEntry> iter = owb.getScoredBridging().iterator(); iter.hasNext();) {
			BufferEntry be = iter.next();
			List<DPart> containingPartitions = owb.getPartitionsFor(be);
			// add this filter if it matches our partitions and we haven't added it already
			// note that because of the outer join test, we'll add some filters multiple times
			if (usedPartitions.containsAll(containingPartitions)) {
				if (!rawFilters.contains(be)) {
					rawFilters.add(be);
					filters.add(be.buildNew(getSchemaContext(),joinStatement.getMapper()));					
				}
				be.attach(outerJoin && !last);
				if (!outerJoin || last) {
					iter.remove();
					any = true;
				}
			}
		}
		return any;
	}
	
	public boolean take(ExplicitJoinBuffer ejb) {
		Set<DPart> usedPartitions = getPartitions();
		boolean any = false;
		for(Iterator<BufferEntry> iter = ejb.getScoredBridging().iterator(); iter.hasNext();) {
			BufferEntry be = iter.next();
			List<DPart> containingPartitions = ejb.getPartitionsFor(be);
			if (usedPartitions.containsAll(containingPartitions)) {
				JoinBufferEntry jbe = (JoinBufferEntry) be;
				ExpressionNode newJoinEx = be.buildNew(getSchemaContext(),joinStatement.getMapper());
				// make sure that we handle any indexes
				IndexCollector.collect(getSchemaContext(), newJoinEx);
				TableInstance targetTable = joinStatement.getMapper().copyForward(jbe.getJoinedTable().getJoinedToTable());
				TableInstance baseTable = joinStatement.getMapper().copyForward(jbe.getJoinedFromKey().toInstance());
				if (targetTable.getTableKey().equals(baseTable.getTableKey()))
					throw new SchemaException(Pass.PLANNER, "Invalid mapping of explicit join: invalid self join");
				JoinedTable jt = new JoinedTable(targetTable, newJoinEx, jbe.getJoinedTable().getJoinType());
				if (be.getTarget() == null) {
					// informal join - we may have multiples for the same target table - don't add duplicates
					Collection<JoinedTable> sub = explicitJoins.get(baseTable);
					if (sub != null) {
						for(JoinedTable ejt : sub) {
							if (ejt.getJoinedToTable().getTableKey().equals(targetTable.getTableKey())) {
								jt = null;
							}
						}
					}
				}
				if (jt != null)
					explicitJoins.put(baseTable, jt);
				clearSelect();
				jbe.attach(false);
				iter.remove();
				any = true;
			}
		}
		return any;
	}
	
	public boolean take(P2ProjectionBuffer proj) {
		Set<DPart> usedPartitions = getPartitions();
		boolean any = false;
		for(Iterator<BufferEntry> iter = proj.getEntries().iterator(); iter.hasNext();) {
			BufferEntry be = iter.next();
			if (be.isRedistRequired()) {
				RedistBufferEntry rbe = (RedistBufferEntry) be;
				List<DPart> containingPartitions = proj.getPartitionsFor(be);
				if (usedPartitions.containsAll(containingPartitions)) {
					ExpressionNode projEx = null;
					if (be.isFor(JoinBufferEntry.class))
						projEx = joinStatement.getMapper().copyForward(rbe.getTarget());
					else
						projEx = be.buildNew(getSchemaContext(), joinStatement.getMapper()); 
					rbe.setMapped(projEx);
					joinStatement.getProjectionEdge().add(projEx);
					clearSelect();
					projectionEntries.add(be);
					iter.remove();
					be.attach(false);
					any = true;
				}
			}
		}
		return any;
	}
	
	@Override
	protected String describe() throws PEException {
		StringBuffer buf = new StringBuffer();
		buf.append("IPE on { ");
		buf.append(Functional.joinToString(entries, ",")).append(" } ")
			.append(getScore());
		return buf.toString();
	}
	
	@Override
	public SelectStatement getJoinQuery(RewriteBuffers projInfo) throws PEException {
		SelectStatement fs = finalSelect;
		SelectStatement q = getJoinQueryInternal(projInfo);
		if (step == null || fs != q) {
			ProjectingFeatureStep pfs = 
					DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(
					plannerContext, 
					factory,
					q,
					getScore(),
					getSourceGroup(),
					q.getDatabase(getSchemaContext()),
					EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(q, getSchemaContext()),
					null,
					getExplain());
			for(PartitionEntry jre : entries) {
				FeatureStep childStep = jre.getStep(null);
				if (!(childStep instanceof RedistFeatureStep))
					childStep.omitSelf();
				pfs.addChild(childStep);
			}
			if (parallelChildren)
				pfs.withParallelChildren();
			setStep(pfs);
		}
		
		return q;
	}

	@Override
	public FeatureStep getStep(RewriteBuffers projInfo) throws PEException {
		if (step == null)
			getJoinQuery(projInfo);
		return step;
	}

	
	private SelectStatement getJoinQueryInternal(RewriteBuffers projInfo) throws PEException {
		if (finalSelect == null) {
			pasteExplicitJoins();
			joinStatement.setWhereClause(ExpressionUtils.buildWhereClause(filters.getExprs()));
			// normalize it, just in case we have unaliased projection entries
			joinStatement.normalize(getSchemaContext());
			pruneProjection();
			finalSelect = joinStatement;
		}
		if (projInfo == null) return finalSelect;
		// note that the projection is no longer in declaration order...so first we're going to sort what's left into
		// declaration order, adding nulls for stuff that doesn't exist.  with that in hand, we can reverse apply p3 -> p2, p2 -> p1, p1 -> orig
		TreeMap<Integer, ExpressionNode> ordered = new TreeMap<Integer, ExpressionNode>();
		TreeMap<Integer, BufferEntry> oe = new TreeMap<Integer, BufferEntry>();
		for(int i = 0; i < projectionEntries.size(); i++) {
			ordered.put(projectionEntries.get(i).getBeforeOffset(), ExpressionUtils.getTarget(finalSelect.getProjectionEdge().get(i)));
			oe.put(projectionEntries.get(i).getBeforeOffset(), projectionEntries.get(i));
		}
		ArrayList<ExpressionNode> inorder = new ArrayList<ExpressionNode>(ordered.values());
		projectionEntries = new ArrayList<BufferEntry>(oe.values());
		List<ExpressionNode> finalProjection = buildFinalProjection(projInfo, inorder);
		finalSelect.setProjection(finalProjection);
		finalSelect.normalize(getSchemaContext());
		
		return finalSelect;
	}

	protected void pasteExplicitJoins() throws PEException {
		if (explicitJoins.isEmpty()) 
			return;
		HashSet<JoinedTable> allJoins = new HashSet<JoinedTable>();
		allJoins.addAll(explicitJoins.values());
		// so we basically have something of the form select ... from A, B, C right now
		// and a bunch of explicit joins keyed by lhs; however the joins may be chained
		// (i.e. the original was A ij B ij C) - so we have to do a deep search down the branches
		// in order to find the point at which the join should be pasted in
		for(TableInstance ti : explicitJoins.keySet()) {
			Collection<JoinedTable> sub = explicitJoins.get(ti);
			if (sub == null || sub.isEmpty())
				continue;
			
			HashSet<TableInstance> rhs = new HashSet<TableInstance>();
			for(FromTableReference ftr : joinStatement.getTables()) {
				if (ftr.getBaseTable() == ti) {
					ftr.addJoinedTable(sub);
					allJoins.removeAll(sub);
					for(JoinedTable jt : sub) {
						if (jt.getJoinedToQuery() != null)
							throw new PEException("Unhandled nested query");
						rhs.add(jt.getJoinedToTable());
					}
				} else if (!ftr.getTableJoins().isEmpty()) {
					// search the joins as well
					for(JoinedTable jt : ftr.getTableJoins()) {
						if (jt.getJoinedToTable() == ti) {
							ftr.addJoinedTable(sub);
							allJoins.removeAll(sub);
							for(JoinedTable ijt : sub) {
								if (ijt.getJoinedToQuery() != null)
									throw new PEException("Unhandled nested query");
								rhs.add(ijt.getJoinedToTable());
							}
						}
					}
				}
			}
			for(Iterator<FromTableReference> iter = joinStatement.getTablesEdge().iterator(); iter.hasNext();) {
				FromTableReference ftr = iter.next();
				if (ftr.getTableJoins().isEmpty() && rhs.contains(ftr.getBaseTable()))
					iter.remove();
			}
		}
		if (!allJoins.isEmpty()) 
			throw new PEException("Unable to paste in some explicit joins on joined partition entry");
		
		fixDuplicatePastes();
	}
	
	protected void pruneProjection() {
		// we're going to note offsets of entries to remove, then remove them in reverse order
		LinkedList<Integer> toRemove = new LinkedList<Integer>();
		for(int i = 0; i < projectionEntries.size(); i++) {
			BufferEntry be = projectionEntries.get(i);
			if (!be.isNeeded())
				toRemove.addFirst(i);
		}
		for(Integer ith : toRemove) {
			projectionEntries.remove(ith.intValue());
			joinStatement.getProjectionEdge().remove(ith.intValue());
		}
		sane("pruning",projectionEntries.size(),joinStatement.getProjectionEdge().size(),joinStatement);
	}
	
	@Override
	protected ExecutionCost computeScore() {
		return specifiedScore;
	}

	@Override
	protected Set<TableKey> computeTables() {
		HashSet<TableKey> tab = new HashSet<TableKey>();
		for(PartitionEntry jre : entries)
			tab.addAll(jre.getSpanningTables());
		return tab;
	}

	@Override
	protected Set<DPart> computePartitions() {
		HashSet<DPart> ret = new HashSet<DPart>();
		for(PartitionEntry jre : entries)
			ret.addAll(jre.getPartitions());
		return ret;
	}

	@Override
	protected PEStorageGroup computeSourceGroup() {
		return currentGroup;
	}

	@Override
	protected Set<DistributionVector> computeDistributedOn() {
		return dists;
	}

	@Override
	public List<ExpressionNode> mapDistributedOn(List<ExpressionNode> in) throws PEException {
		// get the final query for the partition
		SelectStatement finalIpeQuery = getJoinQuery(null);
		
		List<ExpressionNode> mappedDistributeOn = finalIpeQuery.getMapper().copyForward(in);
		return mappedDistributeOn;
	}

	@Override
	public SelectStatement getTempTableSource() throws PEException {
		return getJoinQuery(null);
	}

	protected List<ExpressionNode> buildFinalProjection(RewriteBuffers buffers, List<ExpressionNode> ordered) {
		ArrayList<ExpressionNode> p2version = new ArrayList<ExpressionNode>();
		P2ProjectionBuffer p2proj = buffers.getP2();
		P3ProjectionBuffer p3proj = buffers.getProjectionBuffer();
		P1ProjectionBuffer p1proj = buffers.getP1();
		// any entry in p2proj that is no longer needed need not be considered - so just traverse the p2 version looking for entries
		// still needed, then map via p3proj to the current offset and pick up the expression node there - duping
		for(BufferEntry be2 : p2proj.getEntries()) {
			if (!be2.isNeeded()) continue;
			BufferEntry be3 = p3proj.getP3ForP2(be2);
			ExpressionNode p3v = ordered.get(be3.getBeforeOffset());
			p2version.add((ExpressionNode) p3v.copy(null));
		}
		ArrayList<ExpressionNode> p1version = new ArrayList<ExpressionNode>();
		for(BufferEntry be1 : p1proj.getEntries()) {
			p1version.add(be1.buildNew(p2version));
		}
		ArrayList<ExpressionNode> origVersion = new ArrayList<ExpressionNode>();
		for(BufferEntry ope : buffers.getOriginalProjection().getEntries()) {
			if (ope.getAfterOffsetBegin() == ope.getAfterOffsetEnd()) {
				// data dependent, just add a copy back in
				origVersion.add((ExpressionNode) ope.getTarget().copy(null));
			} else {
				origVersion.add(ope.buildNew(p1version));
			}
		}
		// we've modded the projection, also mod the projection entries to match
		projectionEntries = new ListSet<BufferEntry>(buffers.getOriginalProjection().getEntries());
		return origVersion;
	}

	@Override
	public List<BufferEntry> getBufferEntries() {
		return projectionEntries;
	}

	@Override
	public SelectStatement getCurrentExecQuery() {
		return joinStatement;
	}

	private void fixDuplicatePastes() {
		List<FromTableReference> toBeDeleted = new ArrayList<FromTableReference>();
		List<FromTableReference> allFromTableReferences = joinStatement.getTables();
		for (int i = 0; i < allFromTableReferences.size(); i++) {
			
			if (allFromTableReferences.size() < (i+1))
				break;
			
			FromTableReference currentFromTableReference = allFromTableReferences.get(i);
			
			if (toBeDeleted.contains(currentFromTableReference)) {
				continue;
			}
			
			Set<TableInstance> currentTablesInFromTableReference = getAllTablesInFromTableReference(currentFromTableReference);
			
			for (int j = i+1; j < allFromTableReferences.size(); j++) {
				FromTableReference nextFromTableReference = allFromTableReferences.get(j);
				if (!toBeDeleted.contains(nextFromTableReference)) {
					TableInstance nextTableInstance = nextFromTableReference.getBaseTable();
					if (currentTablesInFromTableReference.contains(nextTableInstance)) {
						currentFromTableReference.addJoinedTable(nextFromTableReference.getTableJoins());
						toBeDeleted.add(nextFromTableReference);
					}
				}
			}
		}
		joinStatement.removeTables(toBeDeleted);
	}
	
	private Set<TableInstance> getAllTablesInFromTableReference(FromTableReference currentFromTableReference) {
		Set<TableInstance> currentTablesInJoin = new HashSet<TableInstance>();
		
		currentTablesInJoin.add(currentFromTableReference.getBaseTable());
		
		if (currentFromTableReference.getTarget() instanceof TableInstance) {
			currentTablesInJoin.add((TableInstance)currentFromTableReference.getTarget());
		} else if (currentFromTableReference.getTarget() instanceof TableJoin) {
			currentTablesInJoin.addAll(Functional.apply(
					((TableJoin)currentFromTableReference.getTarget()).getUnrolledOrder().getSecond(),
					new UnaryFunction<TableInstance, JoinedTable>() {
						@Override
						public TableInstance evaluate(JoinedTable object) {
							return object.getJoinedToTable();
						}
					}));
		}
		
		return currentTablesInJoin;
	}

	@Override
	public List<ExpressionNode> getProjection() throws PEException {
		return joinStatement.getProjection();
	}

	private void clearSelect() {
		finalSelect = null;
		step = null;
	}
	
}