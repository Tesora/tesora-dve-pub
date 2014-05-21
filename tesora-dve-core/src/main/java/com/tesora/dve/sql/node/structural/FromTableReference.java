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
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.Pair;

public class FromTableReference extends StructuralNode {

	// the target is a table instance, subquery, or joined table
	private final SingleEdge<FromTableReference,ExpressionNode> target =
			new SingleEdge<FromTableReference,ExpressionNode>(FromTableReference.class,this,EdgeName.FROM_TARGET);
	
	public FromTableReference(ExpressionNode targ) {
		super(null);
		target.set(targ);
	}

	public TableInstance getBaseTable() {
		if (target.get() instanceof TableInstance) {
			return (TableInstance)target.get();
		} else if (target.get() instanceof TableJoin) {
			return ((TableJoin)target.get()).getBaseTable();
		}
		return null;
	}

	public Subquery getBaseSubquery() {
		if (target.get() instanceof Subquery)
			return (Subquery)target.get();
		return null;
	}

	public List<JoinedTable> getTableJoins() {
		if (target.get() instanceof TableJoin) {
			return ((TableJoin)target.get()).getJoins();
		}
		return Collections.emptyList();
	}
	
	public void addJoinedTable(JoinedTable jt) {
		addJoinedTable(Collections.singletonList(jt));
	}
	
	public void addJoinedTable(Collection<JoinedTable> jt) {
		if (target.get() instanceof TableJoin) {
			TableJoin tj = (TableJoin) target.get();
			tj.getJoinsEdge().addAll(jt);
		} else {
			TableJoin tj = new TableJoin(target.get(),Functional.toList(jt));
			target.set(tj);
		}
		
	}
	
	public void removeJoinedTable(int offset) {
		if (target.get() instanceof TableJoin) {
			TableJoin tj = (TableJoin)target.get();
			tj.getJoinsEdge().remove(offset);
			return;
		}
		throw new IllegalStateException("factor is not a table join");
	}
	
	public ExpressionNode getTarget() {
		return target.get();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return (List<T>) Collections.singletonList(target);
	}
	
	@Override
	public LanguageNode copy(CopyContext in) {
		return new FromTableReference((ExpressionNode)target.get().copy(in));
	}	
	
	@Override
	public String toString() {
		return toString(SchemaContext.threadContext.get());
	}
	
	public String toString(SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
        Singletons.require(HostService.class).getDBNative().getEmitter().emitFromTableReference(sc, this, buf,-1);
		return buf.toString();		
	}

	public Pair<ExpressionNode,List<JoinedTable>> getUnrolledOrder() {
		ExpressionNode base = getTarget();
		if (base instanceof TableJoin) {
			TableJoin tj = (TableJoin) base;
			return tj.getUnrolledOrder();
		} else {
			return new Pair<ExpressionNode,List<JoinedTable>>(base, getTableJoins());			
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
