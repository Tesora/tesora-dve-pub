// OS_STATUS: public
package com.tesora.dve.sql.node.test;



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
