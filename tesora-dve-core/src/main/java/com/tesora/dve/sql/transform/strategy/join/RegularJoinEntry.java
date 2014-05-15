// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;


import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;

class RegularJoinEntry extends BinaryJoinEntry {
	
	DGJoin originalJoin;
	
	public RegularJoinEntry(SchemaContext sc, SelectStatement orig, DGJoin join, OriginalPartitionEntry lhs, OriginalPartitionEntry rhs, 
			JoinRewriteAdaptedTransform jrat, JoinRewriteTransformFactory factory) {
		super(sc, orig, lhs, rhs, jrat, factory);
		originalJoin = join;
	}
	 
	@Override
	public String toString() {
		try {
			return super.toString() + " regular join of " + left + " and " + right + " (score=" + getScore() + ")";
		} catch (PEException pe) {
			return super.toString() + " regular join of " + left + " and " + right + " (missing score)";
		}
	}
	

	@Override
	public DGJoin getJoin() {
		return originalJoin;
	}
		
	public OriginalPartitionEntry getOtherPartition(OriginalPartitionEntry baseEntry) {
		OriginalPartitionEntry other = null;
		if (getLeftPartition() == baseEntry)
			other = getRightPartition();
		else
			other = getLeftPartition();
		
		return other;
	}
	
	@Override
	public JoinedPartitionEntry schedule() throws PEException {
		PEStorageGroup leftGroup = originalJoin.getLeftTable().getAbstractTable().getStorageGroup(getSchemaContext());
		PEStorageGroup rightGroup = originalJoin.getRightTable().getAbstractTable().getStorageGroup(getSchemaContext());

		return new BinaryStrategyBuilder(getPlannerContext(),this, 
				new StrategyTable(left, leftGroup, originalJoin.getLeftTable()),
				new StrategyTable(right, rightGroup, originalJoin.getRightTable())).buildStrategy().build();	
	}

	
	@Override
	public JoinedPartitionEntry schedule(JoinedPartitionEntry head) throws PEException {
		OriginalPartitionEntry rhs = null;
		TableKey leftOrigTable = null;
		TableKey rightOrigTable = null;
		
		if (head.getSpanningTables().containsAll(left.getSpanningTables())) { 
			rhs = right;
			rightOrigTable = originalJoin.getRightTable();
			leftOrigTable = originalJoin.getLeftTable();
		} else { 
			rhs = left;
			rightOrigTable = originalJoin.getLeftTable();
			leftOrigTable = originalJoin.getRightTable();
		}

		return new BinaryStrategyBuilder(getPlannerContext(),this, 
				new StrategyTable(head, head.getSourceGroup(), leftOrigTable),
				new StrategyTable(rhs, rhs.getSourceGroup(), rightOrigTable)).buildStrategy().build();
	}

	@Override
	public JoinedPartitionEntry schedule(List<JoinedPartitionEntry> lipes) throws PEException {
		if (lipes.size() == 1)
			throw new PEException("Attempt to schedule join " + this + " on partition " + lipes.get(0) + " (missing other side)");
		if (lipes.size() > 2)
			throw new PEException("Attempt to schedule more than two partitions in a binary join");

		JoinedPartitionEntry lipe = lipes.get(0);
		JoinedPartitionEntry ripe = lipes.get(1);
		JoinedPartitionEntry lhs = null;
		JoinedPartitionEntry rhs = null;
		if (lipe.getSpanningTables().containsAll(left.getSpanningTables())) {
			lhs = lipe;
			rhs = ripe;
		} else {
			lhs = ripe;
			rhs = lipe;
		}

		return new BinaryStrategyBuilder(getPlannerContext(),this, 
				new StrategyTable(lhs, lhs.getSourceGroup(), originalJoin.getLeftTable()),
				new StrategyTable(rhs, rhs.getSourceGroup(), originalJoin.getRightTable())).buildStrategy().build();
	}
	
}