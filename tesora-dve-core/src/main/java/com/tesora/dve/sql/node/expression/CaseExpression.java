// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transform.CopyContext;

public class CaseExpression extends ExpressionNode {
	
	protected SingleEdge<CaseExpression, ExpressionNode> testExpression =
		new SingleEdge<CaseExpression, ExpressionNode>(CaseExpression.class, this, EdgeName.CASE_TESTCLAUSE);
	protected SingleEdge<CaseExpression, ExpressionNode> elseExpression =
		new SingleEdge<CaseExpression, ExpressionNode>(CaseExpression.class, this, EdgeName.CASE_ELSECLAUSE);
	protected MultiEdge<CaseExpression, WhenClause> whenClauses =
		new MultiEdge<CaseExpression, WhenClause>(CaseExpression.class, this, EdgeName.CASE_WHENCLAUSE);
	@SuppressWarnings("rawtypes")
	protected List edges = new ArrayList();
	
	@SuppressWarnings("unchecked")
	public CaseExpression(ExpressionNode te, ExpressionNode ee, List<WhenClause> whens, SourceLocation orig) {
		super(orig);
		setTestExpression(te);
		setElseExpression(ee);
		setWhenClauses(whens);
		edges.add(testExpression);
		edges.add(elseExpression);
		edges.add(whenClauses);
	}
	
	public ExpressionNode getTestExpression() { return testExpression.get(); }
	public ExpressionNode getElseExpression() { return elseExpression.get(); }
	
	public List<WhenClause> getWhenClauses() { return whenClauses.getMulti(); }
	
	public void setWhenClauses(List<WhenClause> wc) { whenClauses.set(wc); }
	
	public void setTestExpression(ExpressionNode te) { testExpression.set(te); }
	
	public void setElseExpression(ExpressionNode e) { elseExpression.set(e); }

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ArrayList<WhenClause> wcs = new ArrayList<WhenClause>();
		for(WhenClause wc : getWhenClauses()) {
			wcs.add((WhenClause)wc.copy(cc));
		}
		return new CaseExpression(
				(ExpressionNode)(testExpression.has() ? testExpression.get().copy(cc) : null),
		  (elseExpression.has() ? (ExpressionNode)elseExpression.get().copy(cc) : null),
		  wcs, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}

	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return new NameAlias(new UnqualifiedName("casex"));
	}

	@Override
	public List<ExpressionNode> getSubExpressions() {
		ArrayList<ExpressionNode> subs = new ArrayList<ExpressionNode>();
		if (testExpression.has())
			subs.add(testExpression.get());
		subs.addAll(getWhenClauses());
		if (elseExpression.has())
			subs.add(elseExpression.get());
		return subs;
		
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
