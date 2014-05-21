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

import java.util.Collection;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryPredicate;

// the strategy builder looks at a join entry and determines what strategy to use to build 
// the actual join.  as time goes on, this is becoming less and less trivial.
public class BinaryStrategyBuilder {

	// we have the following strategies:
	// [1] regular join:
	// 			redist both partitions to temp group and exec there.
	// [2] lookup table join.  
	//			redist constrained side to temp group, build lookup table on pers group,
	//			exec inner join against lookup table to build unconstrained side,
	//			redist to temp group and exec there
	// [3] bcast to pg join.
	//			redist constrained side to pg bcast, exec on pg
	// [4] bcast oj singlecast to pg join.
	//			exec orig outer join as inner join redist to single pers site, exec
	//			orig outer join between bcast table and temp table
	//
	// figure out which one to use.  the worst strategy is generally [1]
	// [2] can be adequate, but involves many redists.
	// [3] is an optimal join, but can only be used when we're sure the constrained side isn't that big
	//     and when the bcast side can actually be used in a join (i.e. not as the inner table on an outer join)
	// [4] is a specific optimization for bcast loj anything

	private final JoinEntry containing;
	private final StrategyTable left;
	private final StrategyTable right;
	
	private final PlanningConstraint leftConstraint;
	private final PlanningConstraint rightConstraint;

	private final PlannerContext context;
	
	private StrategyTable constrainedSide;
	private PlanningConstraint singleConstraint;
	
	public BinaryStrategyBuilder(PlannerContext pc, JoinEntry rje,
			StrategyTable leftTable, StrategyTable rightTable) throws PEException {
		this.containing = rje;
		this.context = pc;
		this.left = leftTable;
		this.right = rightTable;
		this.leftConstraint = leftTable.getEntry().getScore().getConstraint();
		this.rightConstraint = rightTable.getEntry().getScore().getConstraint();
	}

	private boolean allAreBroadcast(final SchemaContext sc, Collection<TableKey> tabs) {
		return Functional.all(tabs, new UnaryPredicate<TableKey>() {

			@Override
			public boolean test(TableKey object) {
				return object.getAbstractTable().getDistributionVector(sc).isBroadcast();
			}
			
		});
	}
		
	boolean isAllowedOuterJoinBCast(PartitionEntry pe) {
		if (containing.getJoin().getJoin() == null || !containing.getJoin().getJoinType().isOuterJoin()) return true;
		if (containing.getJoin().getJoinType().isFullOuterJoin()) return false;
		Set<DPart> parts = pe.getPartitions();
		ListSet<TableKey> partKeys = new ListSet<TableKey>();
		for(DPart p : parts)
			partKeys.addAll(p.getTables());
		if (containing.getJoin().getJoinType().isLeftOuterJoin()) {
			// if the specified partition is the on the lhs, it cannot be redisted bcast
			return !partKeys.containsAll(containing.getJoin().getLeftTables());
		} else if (containing.getJoin().getJoin().getJoinType().isRightOuterJoin()) {
			return !partKeys.contains(containing.getJoin().getRightTable());
		} else {
			// be safe
			return false;
		}
	}

	private JoinStrategy recordChoice(JoinStrategy in, DMLExplainRecord why) {
		if (containing.getParentTransform().emitting())
			containing.getParentTransform().emit(in.toString() + (why == null ? "" : " because " + why));
		return in;
	}
	
	private JoinStrategy buildRegular(DMLExplainRecord expRecord) {
		return recordChoice(new RegularJoinStrategy(context, containing,left,right,expRecord), expRecord);
	}
	
	private JoinStrategy buildLookup(StrategyTable constrainedSide, DMLExplainRecord why) {
		return recordChoice(new LookupTableJoinStrategy(context, containing,left,right,constrainedSide, why), why);
	}
	
	private JoinStrategy buildOJOpt(DMLExplainRecord expRecord) {
		return recordChoice(new BCastToPGOuterJoinStrategy(context, containing,left,right, expRecord), expRecord);
	}
	
	private JoinStrategy buildBCastOpt(StrategyTable constrainedSide, DMLExplainRecord record) {
		return recordChoice(new BCastToPGJoinStrategy(context, containing,left,right,constrainedSide, record), record);
	}

	private JoinStrategy buildNoConstraintsStrategy() throws PEException {
		// either there aren't any matchable constraints, or we didn't detect them
		// in this case the most we can say is it might be filtered.  thus our choices
		// are either a regular join strategy or the lookup strategy.
		// if we have rowcounts and they're not outrageous, we can do the lookup table strategy		
		boolean lwc = left.getEntry().getScore().hasWhereClause();
		boolean rwc = right.getEntry().getScore().hasWhereClause();
		if ((lwc && rwc) || (!lwc && !rwc))
			// if they both have where clauses just go with normal strategy
			// likewise if neither does we have to go with the normal strategy
			return buildRegular(lwc && rwc ? DMLExplainReason.BOTH_WC_NO_CONSTRAINTS.makeRecord() : DMLExplainReason.BOTH_NO_WC.makeRecord());
		// else: only one has a where clause - use the lookup table strategy
		if (lwc) 
			return buildLookup(left,DMLExplainReason.LEFT_WC_NO_CONSTRAINT_RIGHT_NO_WC.makeRecord());
		return buildLookup(right,DMLExplainReason.LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT.makeRecord());
	}

	// eventually the hint should include the row counts
	private JoinStrategy buildRankedConstraintsStrategy() throws PEException {
		int rank = leftConstraint.compareTo(rightConstraint);
		if (rank < 0)
			return buildLookup(left,DMLExplainReason.BOTH_CONSTRAINED_LEFT_BETTER.makeRecord());
		else if (rank > 0)
			return buildLookup(right,DMLExplainReason.BOTH_CONSTRAINED_RIGHT_BETTER.makeRecord());
		else
			return buildRegular(DMLExplainReason.BOTH_CONSTRAINED_EQUALLY.makeRecord());		
	}
	
	private void findSingleConstraint() {
		if (constrainedSide != null) return;
		if (leftConstraint != null && rightConstraint == null) {
			constrainedSide = left;
			singleConstraint = leftConstraint;
		} else if (leftConstraint == null && rightConstraint != null) {
			constrainedSide = right;
			singleConstraint = rightConstraint;
		}
	}
		
	private JoinStrategy buildInnerJoinStrategy() throws PEException {
		
		// no outer joins are present - so look at constraints
		if (leftConstraint == null && rightConstraint == null) 
			return buildNoConstraintsStrategy();
		
		// figure out if we have one side constrained.
		findSingleConstraint();
			
		if (constrainedSide == null) {
			// both sides are constrained.
			if (leftConstraint.getType().isUnique())
				return buildBCastOpt(left,DMLExplainReason.BOTH_CONSTRAINED_LEFT_UNIQUELY.makeRecord());
			else if (rightConstraint.getType().isUnique())
				return buildBCastOpt(right,DMLExplainReason.BOTH_CONSTRAINED_RIGHT_UNIQUELY.makeRecord());
			return buildRankedConstraintsStrategy();
		}
		
		// one side is constrained, the other side is not.  do a lookup table unless the
		// constrained side is unique - in which case we can do a bcast table
		// we could also do a bcast table if the side that is constrained only has one column in the result set
		if (singleConstraint.getType().isUnique()) {
			return buildBCastOpt(constrainedSide,DMLExplainReason.ONE_SIDE_UNIQUELY_CONSTRAINED.makeRecord());			
		} else if (constrainedSide.getEntry().getProjection().size() == 1) {
			return buildBCastOpt(constrainedSide,DMLExplainReason.ONE_SIDE_CONSTRAINED_ONE_COLUMN.makeRecord());
		} else {
			return buildLookup(constrainedSide,DMLExplainReason.ONE_SIDE_CONSTRAINED.makeRecord());
		}
	}
	
	private JoinStrategy buildOuterJoinStrategy() throws PEException {
		// first - for outer joins certain operations aren't allowed - like redist bcast
		// figure out which side can't be redist bcast here
		boolean leftAllowedBCast = isAllowedOuterJoinBCast(left.getEntry());
		boolean rightAllowedBCast = isAllowedOuterJoinBCast(right.getEntry());
		
		if (leftConstraint == null && rightConstraint == null) 
			return buildNoConstraintsStrategy();

		// figure out if the bcast oj strategy applies.  it does if the inner partition is bcast.
		if (containing.getJoin().getJoinType().isLeftOuterJoin()) {
			if (allAreBroadcast(containing.getSchemaContext(),left.getEntry().getSpanningTables())) {
				// it still might not apply if the right entry is completely unconstrained
				if (leftConstraint == null && !left.getEntry().getScore().hasWhereClause()) {
					// does not apply
				} else {
					return buildOJOpt(null);
				}
			} 
		} else if (containing.getJoin().getJoinType().isRightOuterJoin()) {
			if (containing.getJoin().getRightTable().getAbstractTable().getDistributionVector(containing.getSchemaContext()).isBroadcast()) {
				if (rightConstraint == null && !right.getEntry().getScore().hasWhereClause()) {
					// does not aply
				} else {
					return buildOJOpt(null);
				}
			}
		}
		
		findSingleConstraint();
		
		if (constrainedSide == null) {
			// both have constraints
			if (leftConstraint.getType().isUnique() && leftAllowedBCast)
				return buildBCastOpt(left,
						DMLExplainReason.OJ_BOTH_CONSTRAINED_LEFT_UNIQUE_ALLOWED_BCAST.makeRecord());
			else if (rightConstraint.getType().isUnique() && rightAllowedBCast)
				return buildBCastOpt(right,
						DMLExplainReason.OJ_BOTH_CONSTRAINED_RIGHT_UNIQUE_ALLOWED_BCAST.makeRecord());
			return buildRankedConstraintsStrategy();
		}
		if (singleConstraint.getType().isUnique()) {
			if (constrainedSide == left && leftAllowedBCast) 
				return buildBCastOpt(constrainedSide,
						DMLExplainReason.OJ_LEFT_UNIQUE_ALLOWED_BCAST_RIGHT_UNCONSTRAINED.makeRecord());
			else if (constrainedSide == right && rightAllowedBCast)
				return buildBCastOpt(constrainedSide,
						DMLExplainReason.OJ_RIGHT_UNIQUE_ALLOWED_BCAST_LEFT_UNCONSTRAINED.makeRecord());
			else
				return buildLookup(constrainedSide,
						DMLExplainReason.ONE_SIDE_UNIQUELY_CONSTRAINED.makeRecord());
		}
		return buildLookup(constrainedSide,DMLExplainReason.ONE_SIDE_CONSTRAINED.makeRecord());
	}
	
	public JoinStrategy buildStrategy() throws PEException {

		// eventually when we have statistics we may choose instead to do a regular strategy in
		// place of a lookup strategy in some cases, i.e. when the unconstrained table is small
		// (few hundred rows)

		// ok, if we have definitive row counts, use the lookup strategy
		// we should be more selective about this - for instance if there are a small number of columns we could maybe just
		// do redist bcast and join.
		long lrc = left.getEntry().getScore().getRowCount();
		long rrc = right.getEntry().getScore().getRowCount();
		
		if (lrc > -1 && rrc > -1) {
			// should maybe push this down, use less of a lookup join and more of a redist to bcast
			if (lrc < rrc)
				return buildLookup(left,new DMLExplainRecord(DMLExplainReason.ROW_ESTIMATES,lrc + "(left) < " + rrc + "(right)"));
			else if (rrc < lrc)
				return buildLookup(right,new DMLExplainRecord(DMLExplainReason.ROW_ESTIMATES,lrc + "(left) > " + rrc + "(right)"));
		}		
		
		boolean isOuterJoin = containing.getJoin().getJoinType().isOuterJoin();

		if (!isOuterJoin)
			return buildInnerJoinStrategy();

		return buildOuterJoinStrategy();
	}

}
