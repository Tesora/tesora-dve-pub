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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.CollapsedJoinGraph;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.jg.JoinEdge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ConstraintCollector;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

// we define a bunch of basic terms here:
// the cardinality (card) of an index is the number of unique values in it.
// the cardinality (card) ratio of an index is how many rows each unique value contributes.
// we seek to find the maximal number of rows, which is the happiest case - i.e. all rows have match values.
//
// costing rules are based on the case. 
// no filters:
//   smaller cardinality * lhs card ratio * rhs card ratio
// both sides with filters, no unique indexes
//   lhs row count * rhs row count
// both sides with filters, unique indexes
//   smaller row count
// when only one side is filtered:
//   unfiltered side rowcount is card ratio * filtered side rowcount  
//
// filters are used to build row count estimates.  index cards and card ratios are used to
// propagate estimates when the corresponding side's row count is unknown.
//
// for a given set of joins A join B join C join D join E join F ....
// we first bind all tables with filters, then we start considering joins
// foreach join: if both sides have row counts, build the combined and register the resulting rowcount
// with each table.  if one side has a row count, propagate and register the rowcount with both tables.
// keep going until all that's left are either joins with two unbound tables, or the last join.
// for the unbound table - use the the no filters case.

public final class Costing {

	public static ExecutionCost buildCombinedCost(SchemaContext sc, DGJoin join, ExecutionCost...costs) {
		return getDefaultStrategy(sc,null).buildCombinedCost(sc, join, costs);
	}

	public static ExecutionCost buildBasicCost(SchemaContext sc, SelectStatement ss, boolean singleSite) {
		return getDefaultStrategy(sc,ss).buildBasicCost(sc, ss, singleSite);
	}

	// we have two strategies - the full costing strategy when costing is turned on and cardinalities are known
	// and a structural costing strategy for all other cases
	private static abstract class CostingStrategy {
		
		abstract ExecutionCost buildCombinedCost(SchemaContext sc, DGJoin join, ExecutionCost...costs);
		
		abstract ExecutionCost buildBasicCost(SchemaContext sc, SelectStatement ss, boolean singleSite);

		ListSet<PlanningConstraint> getConstraints(SchemaContext sc, LanguageNode node, boolean partialOK) {
			ConstraintCollector collector = new ConstraintCollector(sc, node, false, true, partialOK);
			return collector.getConstraints();
		}


		
	}

	private static final CostingStrategy dataStrategy = new CardinalityCosting();
	private static final CostingStrategy structureStrategy = new StructureCosting();
	
	private static CostingStrategy getDefaultStrategy(SchemaContext sc, SelectStatement ss) {
		boolean hasCardInfo = true;
		if (ss != null) {
			ListSet<TableKey> allTabs = ss.getAllTableKeys();
			for(TableKey tk : allTabs) {
				if (!tk.getAbstractTable().hasCardinalityInfo(sc)) {
					hasCardInfo = false;
					break;
				}
			}
		}
		
		// use the server setting.  note that if the server setting is for costing, but no cards are present
		// we'll fall back to structure
		if (hasCardInfo && SchemaVariables.getCostBasedPlanning(sc.getConnection())) 
			return dataStrategy;
		return structureStrategy;
	}
	
	private static class CardinalityCosting extends CostingStrategy {

		@Override
		public ExecutionCost buildCombinedCost(SchemaContext sc, DGJoin join,
				ExecutionCost...costs) {
			if (costs == null) return null;
			if (costs.length < 2) return null;
			if (costs.length > 2) {
				// multijoin - do this with structure only
				return structureStrategy.buildCombinedCost(sc, join, costs);
			}
			ExecutionCost leftCost = costs[0];
			ExecutionCost rightCost = costs[1];
			Long result = compute(sc, join, 
					(leftCost.getRowCount() == -1 ? null : leftCost.getRowCount()),
					(rightCost.getRowCount() == -1 ? null : rightCost.getRowCount()),
					new Counters(), 10);
			if (result == null)
				return structureStrategy.buildCombinedCost(sc, join, costs);
			List<PlanningConstraint> all = new ArrayList<PlanningConstraint>();
			if (leftCost.getConstraint() != null)
				all.add(leftCost.getConstraint());
			if (rightCost.getConstraint() != null)
				all.add(rightCost.getConstraint());
			PlanningConstraint best = ConstraintCollector.chooseBest(sc, all);
			
			return new ExecutionCost(best, (result == null ? -1 : result.longValue()),leftCost, rightCost);
		}

		@Override
		public ExecutionCost buildBasicCost(SchemaContext sc,
				SelectStatement ss, boolean singleSite) {
			ListSet<PlanningConstraint> constraints = getConstraints(sc,ss);
			CollapsedJoinGraph cjg = EngineConstant.PARTITIONS.getValue(ss, sc);
			if (cjg.getAllJoins().isEmpty()) {
				PlanningConstraint best = ConstraintCollector.chooseBest(sc,constraints);
				return new ExecutionCost(EngineConstant.WHERECLAUSE.has(ss), singleSite, best, (best == null ? -1 : best.getRowCount()));
			}
			return buildCost(sc,ss,constraints,cjg.getAllJoins(), singleSite);
		}

		private static final EdgeTest[] edges = new EdgeTest[] { EngineConstant.WHERECLAUSE, EngineConstant.FROMCLAUSE };
		
		private ListSet<PlanningConstraint> getConstraints(SchemaContext sc, SelectStatement ss) {
			ListSet<PlanningConstraint> all = new ListSet<PlanningConstraint>();
			for(EdgeTest et : edges) {
				if (et.has(ss)) 
					all.addAll(getConstraints(sc,et.get(ss),true));
			}
			return all;
		}

		
		private ExecutionCost buildCost(SchemaContext sc, 
				SelectStatement ss, 
				ListSet<PlanningConstraint> constraints, 
				ListSet<DGJoin> joins,
				boolean singleSite) {
			MultiMap<TableKey,PlanningConstraint> constraintsByTable = new MultiMap<TableKey,PlanningConstraint>();
			PlanningConstraint best = null;
			for(PlanningConstraint pc : constraints) { 
				constraintsByTable.put(pc.getTableKey(),pc);
				if (best == null) best = pc;
				else if (best.compareTo(pc) >= 1) best = pc;
			}
			ListSet<TableKey> unbound = new ListSet<TableKey>();
			for(DGJoin dgj : joins) {
				unbound.add(dgj.getRightTable());
				unbound.addAll(dgj.getLeftTables());
			}

			ListSet<TableKey> bound = new ListSet<TableKey>();
			HashMap<TableKey,Long> estimates = new HashMap<TableKey,Long>();
			
			// so on estimates, we first traverse the constraints by table key, and find a 'best' constraint for each
			// for temp tables we use the stored row count
			for(Iterator<TableKey> iter = unbound.iterator(); iter.hasNext();) {
				TableKey tk = iter.next();
				Collection<PlanningConstraint> any = constraintsByTable.get(tk);
				if (any == null || any.isEmpty()) continue;
				// choose a best constraint
				PlanningConstraint tb = null;
				for(PlanningConstraint pc : any) {
					if (pc.getRowCount() == -1) continue; 
					if (tb == null) tb = pc;
					else if (tb.compareTo(pc) >= 1) tb = pc;
				}
				if (tb == null) continue;
				estimates.put(tk,best.getRowCount());
				iter.remove();
				bound.add(tk);
			}
			int effort = 0;
			DGJoin lastJoin = null;
			while(joins.size() > 0) {
				int beforeSize = joins.size();
				Counters counters = new Counters();
				for(Iterator<DGJoin> iter = joins.iterator(); iter.hasNext();) {
					DGJoin dgj = iter.next();
					TableKey ltk = dgj.getLeftTable();
					TableKey rtk = dgj.getRightTable();
					// if the estimates are missing but the tables are temp tables, use the row counts from the temp tables
					Long result = compute(sc, dgj,
							getRowCountEstimate(sc,ltk,estimates),
							getRowCountEstimate(sc,rtk,estimates), 
							counters, effort);
					if (result != null) {
						estimates.put(ltk, result);
						estimates.put(rtk, result);
						iter.remove();
						lastJoin = dgj;
					}
				}
				int afterSize = joins.size();
				int effortWas = effort;
				if (afterSize != beforeSize) {
					effort = 0;
					continue;
				}
				effort = counters.computeEffort();
				if (effortWas == effort) {
					lastJoin = null;
					break;
				}
			}
			long result = -1;
			if (lastJoin != null) {
				Long rc = estimates.get(lastJoin.getRightTable());
				if (rc != null)
					result = rc.longValue();
			} else if (best != null) {
				result = best.getRowCount();
			}
			return new ExecutionCost(EngineConstant.WHERECLAUSE.has(ss), singleSite, best, result);
		}

		private Long compute(SchemaContext sc, DGJoin join, Long lrc, Long rrc, Counters counters, int effort) {
			if (lrc != null && rrc != null) {
				return computeBothFiltered(join.getLeftIndex(sc),lrc.longValue(),join.getRightIndex(sc),rrc);
			} else if (lrc == null && rrc == null) {
				counters.bumpTwoUnRes();
				if (effort < 2) return null;
				return computeTwoUnfiltered(sc,join.getLeftIndex(sc),join.getRightIndex(sc));
			} else {
				counters.bumpOneUnRes();
				if (effort < 1) return null;
				return computeOneUnfiltered(sc,join.getLeftIndex(sc),lrc,join.getRightIndex(sc),rrc);
			}
			
		}
		
		
		private long computeBothFiltered(PEKey leftIndex, long lrc, PEKey rightIndex, long rrc) {
			long result = -1;
			if (leftIndex == null || rightIndex == null)
				result = lrc * rrc;
			else {
				if (leftIndex.isUnique() && rightIndex.isUnique()) 
					result = (lrc < rrc ? lrc : rrc);
				else if (leftIndex.isUnique()) 
					result = lrc;
				else if (rightIndex.isUnique())
					result = rrc;
				else
					result = lrc * rrc;
			}
			return result;
		}
		
		private Long computeOneUnfiltered(SchemaContext sc, PEKey leftIndex, Long lrc, PEKey rightIndex, Long rrc) {
			if (lrc == null) {
				if (leftIndex == null) return null;
				long ratio = leftIndex.getCardRatio(sc);
				if (ratio == -1) return null; 
				return rrc.longValue() * ratio;
			} else {
				if (rightIndex == null) return null;
				long ratio = rightIndex.getCardRatio(sc);
				if (ratio == -1) return null;
				return lrc.longValue() * ratio;
			}
		}
		
		private Long computeTwoUnfiltered(SchemaContext sc, PEKey leftIndex, PEKey rightIndex) {
			if (leftIndex == null || rightIndex == null) return null;
			long leftCard = leftIndex.getCardinality();
			long rightCard = rightIndex.getCardinality();
			long leftRatio = leftIndex.getCardRatio(sc);
			long rightRatio = rightIndex.getCardRatio(sc);
			if (leftCard == -1 || rightCard == -1) return null; 
			if (leftRatio == -1 || rightRatio == -1) return null; 
			return leftRatio * rightRatio * (leftCard < rightCard ? leftCard : rightCard);
		}
		
		private Long getRowCountEstimate(SchemaContext sc, TableKey tk, Map<TableKey,Long> estimates) {
			Long out = estimates.get(tk);
			if (out == null && tk.getAbstractTable().isTempTable()) {
				TempTable tt = (TempTable) tk.getAbstractTable().asTable();
				long result = tt.getTableSizeEstimate(sc);
				if (result > -1)
					out = result;
			}
			return out;
		}

		private static class Counters {
			
			private int oneUnRes;
			private int twoUnRes;
			
			public Counters() {
				oneUnRes = 0;
				twoUnRes = 0;
			}
			
			public void bumpOneUnRes() {
				oneUnRes++;
			}
			
			public void bumpTwoUnRes() {
				twoUnRes++;
			}
			
			public int computeEffort() {
				if (oneUnRes > 0) 
					return 1;
				if (twoUnRes > 0)
					return 2;
				return 0;			
			}
		}
		
	}
		
	
	private static class StructureCosting extends CostingStrategy {

		@Override
		public ExecutionCost buildBasicCost(SchemaContext sc,
				SelectStatement ss, boolean singleSite) {
			ListSet<PlanningConstraint> constraints = getConstraints(sc,ss,false);
			PlanningConstraint best = ConstraintCollector.chooseBest(sc,constraints);
			return new ExecutionCost(EngineConstant.WHERECLAUSE.has(ss),singleSite,best,(best == null ? -1 : best.getRowCount()));
		}

		@Override
		public ExecutionCost buildCombinedCost(SchemaContext sc, DGJoin join,
				ExecutionCost...costs) {
			if (costs.length < 2) return null;
			int leftLimit = costs.length - 1;
			ExecutionCost rhc = costs[leftLimit];
			if (rhc == null)
				return null;
			HashMap<TableKey,ExecutionCost> byKey = new HashMap<TableKey,ExecutionCost>();
			for(int i = 0; i < leftLimit; i++) {
				if (costs[i] != null && costs[i].getConstraint() != null)
					byKey.put(costs[i].getConstraint().getTableKey(),costs[i]);
			}
			List<ExecutionCost> promoted = new ArrayList<ExecutionCost>();
			boolean multiSite = false;
			boolean wc = false;
			for(JoinEdge je : join.getEdges()) {
				ExecutionCost lhc = byKey.get(je.getLHSTab());
				if (lhc == null) continue;
				ExecutionCost c = promoteConstraint(sc, je, lhc, rhc);
				if (c != null) {
					if (!lhc.isSingleSite() || !rhc.isSingleSite())
						multiSite = true;
					if (lhc.hasWhereClause() || rhc.hasWhereClause())
						wc = true;
					promoted.add(c);
				}
			}
			if (promoted.isEmpty()) {
				List<PlanningConstraint> all = new ArrayList<PlanningConstraint>();
				for(ExecutionCost ec : costs)
					if (ec.getConstraint() != null)
						all.add(ec.getConstraint());
				PlanningConstraint best = ConstraintCollector.chooseBest(sc, all);
				return new ExecutionCost(best, -1, costs);
			}
			Collections.sort(promoted);
			if (join.getJoinType().isOuterJoin()) {
				return new ExecutionCost(wc,!multiSite,promoted.get(promoted.size() - 1));
			}
			return new ExecutionCost(wc,!multiSite,promoted.get(0));
		}

		private ListSet<PEKey> buildMatching(SchemaContext sc, ListSet<PEColumn> cols) {
			ListSet<PEKey> candidates = new ListSet<PEKey>();
			for(PEColumn pec : cols) {
				List<PEKey> refs = pec.getReferencedBy(sc);
				for(PEKey pek : refs) {
					HashSet<PEColumn> uses = new HashSet<PEColumn>(pek.getColumns(sc));
					uses.removeAll(cols);
					if (uses.isEmpty()) 
						candidates.add(pek);
				}
			}
			return candidates;
		}

		private ExecutionCost promoteConstraint(SchemaContext sc, JoinEdge je, ExecutionCost lhc, ExecutionCost rhc) {
			ListSet<PEColumn> leftCols = new ListSet<PEColumn>();
			ListSet<PEColumn> rightCols = new ListSet<PEColumn>();
			for(Pair<ColumnInstance,ColumnInstance> p : je.getSimpleColumns()) {
				leftCols.add(p.getFirst().getPEColumn());
				rightCols.add(p.getSecond().getPEColumn());
			}
			ListSet<PEKey> lmatch = buildMatching(sc, leftCols);
			ListSet<PEKey> rmatch = buildMatching(sc, rightCols);

			PEKey luniq = null;
			PEKey runiq = null;
			PEKey lindex = null;
			PEKey rindex = null;
			for(PEKey p : lmatch) {
				if (luniq == null && (p.isUnique()))
					luniq = p;
				else if (!p.isUnique()) {
					if (lindex == null) lindex = p;
					else if (p.getCardRatio(sc) < lindex.getCardRatio(sc)) lindex = p;
				}
			}
			for(PEKey p : rmatch) {
				if (runiq == null && (p.isUnique()))
					runiq = p;
				else if (!p.isUnique()) {
					if (rindex == null) rindex = p;
					else if (p.getCardRatio(sc) < rindex.getCardRatio(sc)) rindex = p;
				}
			}
			long lrc = -1;
			long rrc = -1;
			if (lhc.getRowCount() > -1) {
				if (luniq != null)
					lrc = lhc.getRowCount();
				else if (lindex != null)
					lrc = lhc.getRowCount() * lindex.getCardRatio(sc);
			}
			if (rhc.getRowCount() > -1) {
				if (runiq != null)
					rrc = rhc.getRowCount();
				else if (rindex != null)
					rrc = rhc.getRowCount() * rindex.getCardRatio(sc);
			}
			if (luniq != null && runiq != null) {
				return (lhc.compareTo(rhc) < 0 ? lhc : rhc);
			} else if (luniq != null && rindex != null) {
				return new ExecutionCost(rhc,rrc);
			} else if (lindex != null && runiq != null) {
				return new ExecutionCost(lhc,lrc);
			} else if (lindex != null && rindex != null) {
				long nrc = -1;
				if (lrc > -1 && rrc > -1) nrc = lrc * rrc;
				return new ExecutionCost((lhc.compareTo(rhc) > 0 ? rhc : lhc),nrc);
			} else if (lindex == null) {
				return lhc;
			} else if (rindex == null) {
				return rhc;
			} else {
				return (lhc.compareTo(rhc) > 0 ? rhc : lhc);
			}			
		}
		
		
	}
}
