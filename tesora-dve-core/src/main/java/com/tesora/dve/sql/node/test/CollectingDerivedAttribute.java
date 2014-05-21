// OS_STATUS: public
package com.tesora.dve.sql.node.test;

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



import com.tesora.dve.sql.node.AbstractTraversal.Order;
import com.tesora.dve.sql.node.CollectingTraversal;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

class CollectingDerivedAttribute extends DerivedAttribute<ListSet<LanguageNode>> {

	protected final EdgeTest edge;
	protected final NodeTest<LanguageNode> matching;
	protected final Order order;
	
	public CollectingDerivedAttribute(Order travOrder, EdgeTest edge, NodeTest<LanguageNode> nt) {
		this.edge = edge;
		this.matching = nt;
		order = travOrder;
	}
	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (edge.has(ln));
	}
	
	@Override
	public ListSet<LanguageNode> computeValue(SchemaContext sc, LanguageNode ln) {
		CollectingTraversal cta = new CollectingTraversal(order,matching);
		Edge<?, LanguageNode> e = edge.getEdge(ln);
		cta.traverse(e);
		return cta.getCollected();
	}
	
	
}
