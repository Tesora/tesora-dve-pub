// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

import java.util.List;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;

public abstract class ProjectingStatement extends MultiTableDMLStatement {

	protected MultiEdge<ProjectingStatement, SortingSpecification> orderBys =
			new MultiEdge<ProjectingStatement, SortingSpecification>(ProjectingStatement.class, this, EdgeName.ORDERBY);
	protected SingleEdge<ProjectingStatement, LimitSpecification> limitExpression =
			new SingleEdge<ProjectingStatement, LimitSpecification>(ProjectingStatement.class, this, EdgeName.LIMIT);

	protected boolean grouped;
	
	
	protected ProjectingStatement(SourceLocation loc) {
		super(loc);
	}

	// this must return projections in left to right order
	public abstract List<List<ExpressionNode>> getProjections();

	// normalize the given projection - but do not modify self.  this is used in union statement processing.
	public abstract List<ExpressionNode> normalize(SchemaContext sc, List<ExpressionNode> in);
	
	public LimitSpecification getLimit() { return limitExpression.get(); }
	public ProjectingStatement setLimit(LimitSpecification ls) { limitExpression.set(ls); return this;}
	public Edge<ProjectingStatement, LimitSpecification> getLimitEdge() { return limitExpression; }

	public List<SortingSpecification> getOrderBys() { return orderBys.getMulti(); }
	public MultiEdge<ProjectingStatement, SortingSpecification> getOrderBysEdge() { return orderBys; }
	public ProjectingStatement setOrderBy(List<SortingSpecification> order) { 
		orderBys.set(order);
		SortingSpecification.setOrdering(order, Boolean.TRUE);
		return this;
	}
	
	public boolean isGrouped() {
		return grouped;
	}
	
	public void setGrouped(boolean v) {
		grouped = v;
	}
	
	// used in union support
	protected abstract SelectStatement findLeftmostSelect();
	
}
