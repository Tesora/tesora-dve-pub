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

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.StructuralNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.CopyContext;

public class JoinedTable extends StructuralNode {

	// for A a join B b on f(a,b)
	// b is joinedTo, f(a,b) is joinOn, join is joinSpec
	private final SingleEdge<JoinedTable, ExpressionNode> joinedTo =
		new SingleEdge<JoinedTable, ExpressionNode>(JoinedTable.class, this, EdgeName.JOIN_TO);
	private final SingleEdge<JoinedTable, ExpressionNode> joinOn =
		new SingleEdge<JoinedTable, ExpressionNode>(JoinedTable.class, this, EdgeName.JOIN_ON);
	private JoinSpecification joinSpec;
	private List<Name> usingColSpec = new ArrayList<Name>();
	
	@SuppressWarnings("rawtypes")
	private final List edges = new ArrayList();
	
	public JoinedTable(ExpressionNode target, ExpressionNode on, JoinSpecification joinSpec) {
		this(target, on, joinSpec, null);
	}
	
	@SuppressWarnings("unchecked")
	public JoinedTable(ExpressionNode target, ExpressionNode on, JoinSpecification joinSpec, List<Name> usingColSpec) {
		super(null);
		this.joinedTo.set(target);
		this.joinOn.set(on);
		this.joinSpec = joinSpec;
		edges.add(joinedTo);
		edges.add(joinOn);
		if (usingColSpec != null) {
			this.usingColSpec.addAll(usingColSpec);
		}
	}

	public ExpressionNode getJoinedTo() { return joinedTo.get(); }
	public JoinSpecification getJoinType() { return joinSpec; }
	public void setJoinType(JoinSpecification js) { joinSpec = js; }
	
	public ExpressionNode getJoinOn() { return joinOn.get(); }
	
	public Edge<?,ExpressionNode> getJoinOnEdge() { return joinOn; }
	
	public TableInstance getJoinedToTable() {
		if (getJoinedTo() instanceof TableInstance)
			return (TableInstance)getJoinedTo();
		return null;
	}

	public Subquery getJoinedToQuery() {
		if (getJoinedTo() instanceof Subquery)
			return (Subquery)getJoinedTo();
		return null;
	}

	// parent queries
	public FromTableReference getEnclosingFromTableReference() {
		return (FromTableReference) getEnclosing(FromTableReference.class, null);
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}

	@Override
	public LanguageNode copy(CopyContext in) {
		return new JoinedTable(
				joinedTo.has() ? (ExpressionNode)joinedTo.get().copy(in) : null,
				joinOn.has() ? (ExpressionNode)joinOn.get().copy(in) : null,
				joinSpec,
				usingColSpec);		

	}

	public List<Name> getUsingColSpec() {
		return usingColSpec;
	}

	public void setUsingColSpec(List<Name> usingColSpec) {
		this.usingColSpec = usingColSpec;
	}

	public boolean hasUsingSpec() {
		return !usingColSpec.isEmpty();
	}

	@Override
	public String toString() {
		return toString(SchemaContext.threadContext.get());
	}
	
	public String toString(SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
        Singletons.require(HostService.class).getDBNative().getEmitter().emitJoinedTable(sc, sc.getValues(),this, buf,-1);
		return buf.toString();		
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		JoinedTable ojt = (JoinedTable) other;
		return joinSpec.equals(ojt.joinSpec);
	}

	@Override
	protected int selfHashCode() {
		return joinSpec.hashCode();
	}

	
}
