// OS_STATUS: public
package com.tesora.dve.sql.node.structural;

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



import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.StructuralNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.CopyContext;

public class LimitSpecification extends StructuralNode {

	private final SingleEdge<LimitSpecification,ExpressionNode> offset =
		new SingleEdge<LimitSpecification,ExpressionNode>(LimitSpecification.class, this, EdgeName.LIMIT_OFFSET);
	private final SingleEdge<LimitSpecification,ExpressionNode> rowcount =
		new SingleEdge<LimitSpecification,ExpressionNode>(LimitSpecification.class, this, EdgeName.LIMIT_ROWCOUNT);
	@SuppressWarnings("rawtypes")
	private final List edges = new ArrayList();
	
	@SuppressWarnings("unchecked")
	public LimitSpecification(ExpressionNode rows, ExpressionNode off) {
		super(null);
		this.offset.set(off);
		this.rowcount.set(rows);
		edges.add(offset);
		edges.add(rowcount);
	}

	public ExpressionNode getRowcount() { return rowcount.get(); }
	public ExpressionNode getOffset() { return offset.get(); }

	public Edge<LimitSpecification,ExpressionNode> getOffsetEdge() { return offset; }
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}
	
	@Override
	public LanguageNode copy(CopyContext in) {
		return new LimitSpecification(
				rowcount.has() ? (ExpressionNode)rowcount.get().copy(in) : null,
				offset.has() ? (ExpressionNode)offset.get().copy(in) : null);		
	}

	public boolean hasLimitOne(SchemaContext sc) {
		boolean ret = false;
		
		if (rowcount.get() instanceof LiteralExpression) {
			ret = ((Long)((LiteralExpression)rowcount.get()).getValue(sc)) == 1;
		}
		
		return ret;
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
