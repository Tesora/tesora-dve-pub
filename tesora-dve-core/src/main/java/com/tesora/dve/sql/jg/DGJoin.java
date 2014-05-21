package com.tesora.dve.sql.jg;

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

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class DGJoin {

	JoinedTable enclosingJoin;
	
	ListSet<JoinEdge> forEdges;
	
	ListSet<TableKey> lhs;
	TableKey rhs;
	
	JoinPosition position;
	
	public DGJoin(JoinedTable enclosing, int branch, int offset, boolean fixedLocation, ListSet<TableKey> leftTabs, TableKey rightTab) {
		enclosingJoin = enclosing;
		if (enclosing != null)
			position = new JoinPosition(branch,offset, fixedLocation);
		else
			position = new JoinPosition();
		forEdges = new ListSet<JoinEdge>();
		lhs = leftTabs;
		rhs = rightTab;
	}
	
	public ListSet<TableKey> getLeftTables() {
		return lhs;
	}
	
	public TableKey getLeftTable() {
		if (lhs.size() > 1)
			throw new SchemaException(Pass.PLANNER, "attempt to get left table from multileft tables");
		return lhs.get(0);
	}
	
	public TableKey getRightTable() {
		return rhs;
	}
	
	public JoinPosition getPosition() {
		return position;
	}
	
	public void describe(SchemaContext sc, StringBuilder buf) {
		if (enclosingJoin == null)
			buf.append("inner (null) join");
		else
			buf.append(enclosingJoin.getJoinType().getSQL());
		if (position.getBranch() > -1)
			buf.append(" ").append(position);
	}
	
	public boolean isMultijoin() {
		return false;
	}

	public JoinedTable getJoin() {
		return enclosingJoin;
	}
	
	public boolean isInnerJoin() {
		return enclosingJoin == null || enclosingJoin.getJoinType().isInnerJoin();
	}
	
	public JoinSpecification getJoinType() {
		if (enclosingJoin == null)
			return JoinSpecification.INNER_JOIN;
		else
			return enclosingJoin.getJoinType();
	}
	
	public void addEdge(JoinEdge je) {
		forEdges.add(je);
	}
	
	public ListSet<JoinEdge> getEdges() {
		return forEdges;
	}

	JoinEdge getSingleEdge() {
		if (forEdges.size() > 1)
			throw new SchemaException(Pass.PLANNER, "attempt to get single edge from multiedge");
		return forEdges.get(0);
	}
	
	public List<ExpressionNode> getRedistJoinExpressions(final TableKey forTable) {
		return getSingleEdge().getRedistJoinExpressions(forTable);
	}
	
	public boolean isSimpleJoin() {
		return getSingleEdge().isSimpleJoin();
	}
	
	public PEKey getRightIndex(SchemaContext sc) {
		return forEdges.get(0).getRightKey(sc);
	}
	
	public PEKey getLeftIndex(SchemaContext sc) {
		return forEdges.get(0).getLeftKey(sc);
	}
	
}
