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


import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.StructuralNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.Accessor;

public class SortingSpecification extends StructuralNode {

	private final SingleEdge<SortingSpecification, ExpressionNode> target =
		new SingleEdge<SortingSpecification, ExpressionNode>(SortingSpecification.class, this, EdgeName.SORT_TARGET);
	
	private final boolean direction;
	private Boolean ordering = null;
	
	public SortingSpecification(ExpressionNode e, boolean asc) {
		super(null);
		this.target.set(e);
		this.direction = asc;
	}

	public ExpressionNode getTarget() { return target.get(); }
	public boolean isAscending() { return direction; }
	
	public SingleEdge<SortingSpecification, ExpressionNode> getTargetEdge() {
		return target;
	}
	
	public void setOrdering(Boolean b) {
		ordering = b;
	}
	
	public Boolean isOrdering() {
		return ordering;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return (List<T>) Collections.singletonList(target);
	}

	
	public static void setOrdering(Collection<SortingSpecification> c, Boolean v) {
		if (c == null) return;
		for(SortingSpecification ss : c)
			ss.setOrdering(v);
	}

	@Override
	public LanguageNode copy(CopyContext in) {
		SortingSpecification ss = new SortingSpecification((target.has() ? (ExpressionNode)target.get().copy(in) : null), direction);
		ss.setOrdering(ordering);
		return ss;		
	}

	public static final Accessor<ExpressionNode, SortingSpecification> getTarget = new Accessor<ExpressionNode, SortingSpecification>() {

		@Override
		public ExpressionNode evaluate(SortingSpecification object) {
			return object.getTarget();
		}
		
	};

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		SortingSpecification oss = (SortingSpecification) other;
		if ((direction && oss.direction) || (!direction && !oss.direction)) {
			return !ObjectUtils.equals(ordering, oss.ordering);
		}
		return false;
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(addSchemaHash(1,direction),(ordering == null ? false : ordering.booleanValue()));
	}
	
}
