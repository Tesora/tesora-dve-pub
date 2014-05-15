// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.HashSet;
import java.util.List;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.jg.DGMultiJoin;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public class MultiJoinEntry extends JoinEntry {

	ListSet<OriginalPartitionEntry> lhs;
	OriginalPartitionEntry rhs;
	DGMultiJoin mjoin;
	
	public MultiJoinEntry(SchemaContext sc, SelectStatement basis, ListSet<OriginalPartitionEntry> lhs, DGMultiJoin theJoin, OriginalPartitionEntry rhs,
			JoinRewriteAdaptedTransform parent, JoinRewriteTransformFactory factory) {
		super(parent.getPlannerContext(),sc, basis, parent, factory);
		this.lhs = lhs;
		this.rhs = rhs;
		this.mjoin = theJoin;
	}

	@Override
	protected ExecutionCost computeScore() {
		return ExecutionCost.maximize(Functional.apply(lhs, new UnaryFunction<ExecutionCost,OriginalPartitionEntry>() {

			@Override
			public ExecutionCost evaluate(OriginalPartitionEntry object) {
				try {
					return object.getScore();						
				} catch (PEException pe) {
					throw new SchemaException(Pass.PLANNER, "cost not available",pe);
				}
			}
			
		}));
	}

	@Override
	public ListSet<DPart> getPartitions() {
		ListSet<DPart> out = new ListSet<DPart>();
		for(OriginalPartitionEntry pe : lhs)
			out.addAll(pe.getPartitions());
		out.addAll(rhs.getPartitions());
		return out;
	}

	@Override
	public DGJoin getJoin() {
		return mjoin;
	}

	@Override
	public JoinedPartitionEntry schedule()
			throws PEException {
		// can only do this if lhs only has one partition in it
		if (lhs.size() > 1)
			throw new PEException("Invalid use of multijoin init schedule");
		OriginalPartitionEntry left = lhs.get(0);
		PEStorageGroup leftGroup = left.getSourceGroup();

		return new BinaryStrategyBuilder(getPlannerContext(),this, 
				new StrategyTable(left, leftGroup, mjoin.getLeftTables()),
				new StrategyTable(rhs, rhs.getSourceGroup(), mjoin.getRightTable())).buildStrategy().build();	
	}


	@Override
	public JoinedPartitionEntry schedule(List<JoinedPartitionEntry> ipes) throws PEException {
		throw new PEException("schedule(TempGroupManager,List<IntermediatePartitionEntry>)");
	}

	@Override
	protected boolean preferNewHead(ListSet<JoinedPartitionEntry> head) {
		return false;
	}

	@Override
	protected boolean tryNewHead(ListSet<JoinedPartitionEntry> heads) throws PEException {
		if (parentTransform.canschedule(this)) {
			JoinedPartitionEntry ipe = schedule(); 
			heads.add(ipe);
			if (parentTransform.emitting()) {
				parentTransform.emit("Created initial ipe: ");
				parentTransform.emit(ipe.toString());
			}
			return true;				
		}
		return false;
	}
	

	
	@Override
	public boolean buildIndIntJoin(ListSet<JoinedPartitionEntry> heads) throws PEException {
		// for now we require that the lhs be an intermediate partition - thus we only need to find
		// the ipe that contains all of the lhs parts but not the rhs part
		HashSet<DPart> lhsParts = new HashSet<DPart>();
		for(OriginalPartitionEntry pe : lhs)
			lhsParts.addAll(pe.getPartitions());
		for(JoinedPartitionEntry ipe : heads) {
			if (ipe.getPartitions().containsAll(lhsParts) && !ipe.getPartitions().contains(rhs.getPartition())) {
				if (parentTransform.canschedule(this)) {
					JoinedPartitionEntry newHead = schedule(ipe);
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

	@Override
	public JoinedPartitionEntry schedule(JoinedPartitionEntry head) throws PEException {
		return new BinaryStrategyBuilder(getPlannerContext(),this,
				new StrategyTable(head,head.getSourceGroup(),mjoin.getLeftTables()),
				new StrategyTable(rhs,rhs.getSourceGroup(),mjoin.getRightTable())
				).buildStrategy().build();
		
	}		
}
