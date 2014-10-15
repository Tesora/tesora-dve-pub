package com.tesora.dve.sql.statement.dml;

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
import com.tesora.dve.sql.schema.TriggerEvent;

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
	
	@Override
	public TriggerEvent getTriggerEvent() {
		return null;
	}

}
