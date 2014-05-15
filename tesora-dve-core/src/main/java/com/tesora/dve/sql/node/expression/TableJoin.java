// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.Pair;

public class TableJoin extends ExpressionNode {
	
	private final SingleEdge<TableJoin,ExpressionNode> tableFactor =
		new SingleEdge<TableJoin,ExpressionNode>(TableJoin.class, this, EdgeName.JOIN_BASE);
	private final MultiEdge<TableJoin,JoinedTable> joins =
		new MultiEdge<TableJoin,JoinedTable>(TableJoin.class, this, EdgeName.JOIN_JOINS);
	@SuppressWarnings("rawtypes")
	private final List edges = new ArrayList();

	
	@SuppressWarnings("unchecked")
	public TableJoin(ExpressionNode tableFactor, List<JoinedTable> joins, SourceLocation tree) {
		super(tree);
		setFactor(tableFactor);
		setJoins(joins);
		edges.add(this.tableFactor);
		edges.add(this.joins);
	}

	public TableJoin(ExpressionNode tableFactor, List<JoinedTable> joins) {
		this(tableFactor,joins,null);
	}
	
	public TableJoin(ExpressionNode tableFactor, JoinedTable jt) {
		this(tableFactor,Collections.singletonList(jt));
	}
	
	public void setFactor(ExpressionNode en) {
		if (en == null)
			throw new IllegalArgumentException("Missing factor");
		tableFactor.set(en);
	}
	
	public void setJoins(List<JoinedTable> joins) {
		this.joins.set(joins);
	}
	
	public ExpressionNode getFactor() {
		return tableFactor.get();
	}
	
	public List<JoinedTable> getJoins() {
		return joins.getMulti();
	}
	
	public Edge<TableJoin,ExpressionNode> getFactorEdge() {
		return tableFactor;
	}
	
	public MultiEdge<TableJoin,JoinedTable> getJoinsEdge() {
		return joins;
	}
	
	public TableInstance getBaseTable() {
		if (tableFactor.get() instanceof TableInstance)
			return (TableInstance)tableFactor.get();
		return null;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ArrayList<JoinedTable> cj = new ArrayList<JoinedTable>();
		if (joins.has()) {
			for(JoinedTable jt : joins.getMulti()) {
				cj.add((JoinedTable) jt.copy(cc));
			}			
		}
		return new TableJoin((ExpressionNode)tableFactor.get().copy(cc),cj,getSourceLocation());
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<? extends Edge<?,?>> getEdges() {
		return edges;
	}

	public Pair<ExpressionNode,List<JoinedTable>> getUnrolledOrder() {
		ExpressionNode base = getFactor();
		if (base instanceof TableJoin) {
			TableJoin r = (TableJoin) base;
			Pair<ExpressionNode,List<JoinedTable>> sub = r.getUnrolledOrder();
			ArrayList<JoinedTable> allJoins = new ArrayList<JoinedTable>();
			allJoins.addAll(sub.getSecond());
			allJoins.addAll(getJoins());
			return new Pair<ExpressionNode,List<JoinedTable>>(sub.getFirst(),allJoins);
		} else {
			return new Pair<ExpressionNode,List<JoinedTable>>(base, getJoins());
		}
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return true;
	}

	@Override
	protected int selfHashCode() {
		return 0;
	}
	
}
