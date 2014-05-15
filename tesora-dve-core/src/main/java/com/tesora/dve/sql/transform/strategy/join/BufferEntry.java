// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.util.ListSet;

public class BufferEntry {
	
	protected final ExpressionNode node;
	protected ListSet<BufferEntry> requiredBy;
	protected ListSet<BufferEntry> requires;
	
	protected int beforeOffset;
	protected int afterOffsetBegin;
	protected int afterOffsetEnd;
	
	protected Map<ExpressionNode,RedistBufferEntry> compoundRedists = new HashMap<ExpressionNode,RedistBufferEntry>();

	public BufferEntry(ExpressionNode targ) {
		if (targ instanceof ExpressionAlias)
			throw new SchemaException(Pass.PLANNER, "Illegal join buffer entry: alias");
		node = targ;
		beforeOffset = -1;
		afterOffsetBegin = -1;
		afterOffsetEnd = -1;
		requiredBy = null;
		requires = null;
	}
	
	public ExpressionNode getTarget() {
		return node;
	}
		
	// add the arg to my list of requires, add myself to the arg's list of requiredBys
	public void addDependency(BufferEntry be) {
		if (requires == null) requires = new ListSet<BufferEntry>();
		requires.add(be);
		if (be.requiredBy == null) be.requiredBy = new ListSet<BufferEntry>();
		be.requiredBy.add(this);
	}
	
	private void removeDependency(BufferEntry be) { 
		if (requiredBy == null) return;
		requiredBy.remove(be);
		if (requiredBy.isEmpty()) {
			if (requires == null || requires.isEmpty()) return;
			// also clear whatever we depend on
			for(BufferEntry cbe : requires)
				cbe.removeDependency(this);
			requires.clear();
		}
	}
	
	public boolean hasCompoundExpressionDependent() {
		if (requiredBy == null) return false;
		if (requiredBy.isEmpty()) return false;
		for(BufferEntry be : requiredBy)
			if (be.isCompound())
				return true;
		for(BufferEntry be : requiredBy)
			if (be.hasCompoundExpressionDependent())
				return true;
		return false;
	}
	
	public boolean isFor(Class<?> implClass) {
		if (requiredBy == null) return false;
		for(BufferEntry be : requiredBy) {
			if (implClass.isInstance(be))
				return true;
		}
		return false;
	}
	
	public boolean isInvisible() {
		if (!(node instanceof ColumnInstance)) return false;
		ColumnInstance ci = (ColumnInstance) node;
		if (!ci.getColumn().getType().isFloatType()) return false;
		if (!hasCompoundExpressionDependent()) return false;
		return true;
	}
	
	public void attach(boolean maybeDupe) {
		if (requires == null || maybeDupe) return;
		for(BufferEntry be : requires) {
			be.removeDependency(this);
		}
		requires.clear();
	}
	
	public boolean isNeeded() {
		return !(requiredBy == null || requiredBy.isEmpty());
	}
	
	public void setBeforeOffset(int i) { beforeOffset = i; }
	public int getBeforeOffset() { return beforeOffset; }
	
	public void setAfterOffsetBegin(int i) { afterOffsetBegin = i; }
	public int getAfterOffsetBegin() { return afterOffsetBegin; }
	
	public void setAfterOffsetEnd(int i) { afterOffsetEnd = i; }
	public int getAfterOffsetEnd() { return afterOffsetEnd; }
	
	public List<ExpressionNode> getNext() {
		return Collections.singletonList(node);
	}

	public ExpressionNode buildNew(SchemaContext sc, SchemaMapper in) {
		if (compoundRedists.isEmpty())
			return (ExpressionNode) node.copy(in.getCopyContext());
		return buildNewCompoundRedist(sc, in, node);
	}	
	
	public ColumnInstance buildNewCompoundRedist(SchemaContext sc, SchemaMapper sm, ExpressionNode en) {
		RedistBufferEntry rbe = compoundRedists.get(en);
		ColumnKey ck = null;
		if (rbe != null) 
			ck = sm.mapExpressionToColumn(rbe.getMapped());
		if (ck == null)
			ck = sm.mapExpressionToColumn(en);
		if (ck == null) 
			throw new SchemaException(Pass.PLANNER, "Missing compound expr mapping");
		ColumnInstance nci = ck.toInstance();
		return nci;
	}
	
	public ExpressionNode buildNew(List<ExpressionNode> intermediateProjection) {
		return ExpressionUtils.getTarget(intermediateProjection.get(getAfterOffsetBegin()));
	}
	
	@Override
	public String toString() {
		return "BufferEntry{" + node.toString() + "}";
	}
	
	public boolean isCompound() {
		return false;
	}	
	
	public boolean isRedistRequired() {
		return !compoundRedists.isEmpty();
	}
	
	public void registerCompoundRedist(ExpressionNode en, RedistBufferEntry repr) {
		compoundRedists.put(en, repr);
	}

}