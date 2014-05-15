// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.strategy.Costing;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;
import com.tesora.dve.sql.util.ListSet;

/*
 * Represents a join between two (original) partitions.
 */
abstract class BinaryJoinEntry extends JoinEntry {
	
	protected OriginalPartitionEntry left;
	protected OriginalPartitionEntry right;

	public BinaryJoinEntry(SchemaContext sc, SelectStatement orig, OriginalPartitionEntry lhs, OriginalPartitionEntry rhs, 
			JoinRewriteAdaptedTransform jrat, JoinRewriteTransformFactory factory) {
		super(jrat.getPlannerContext(),sc, orig,jrat, factory);
		left = lhs;
		right = rhs;
		if (left == null || right == null)
			throw new SchemaException(Pass.PLANNER, "Missing partition in binary join");
		left.addReferencingJoin(this);
		right.addReferencingJoin(this);
	}
	
	public OriginalPartitionEntry getLeftPartition() {
		return left;
	}
	
	public OriginalPartitionEntry getRightPartition() {
		return right;
	}

	@Override
	protected ExecutionCost computeScore() throws PEException {
		return combineScores(getSchemaContext(), left.getScore(), right.getScore(), getJoin());
	}
	
	public static ExecutionCost combineScores(SchemaContext sc, ExecutionCost left, ExecutionCost right, DGJoin join) {
		if (join == null) {
			// worst case - multiply two together
			long lhs = left.getRowCount();
			long rhs = right.getRowCount();
			return new ExecutionCost(null, (lhs > -1 && rhs > -1 ? lhs * rhs : -1), left, right);
		} else {
			return Costing.buildCombinedCost(sc, join, left, right);
		}
	}
	
	@Override
	public ListSet<DPart> getPartitions() {
		ListSet<DPart> ret = new ListSet<DPart>();
		ret.add(left.getPartition());
		ret.add(right.getPartition());
		return ret;
	}
	
	@Override
	protected boolean preferNewHead(ListSet<JoinedPartitionEntry> head) throws PEException {
		ExecutionCost leftScore = left.getScore();
		ExecutionCost rightScore = right.getScore();
		if (leftScore.getConstraint() != null && rightScore.getConstraint() != null) {
			return true;
		}
		return false;
	}


}
