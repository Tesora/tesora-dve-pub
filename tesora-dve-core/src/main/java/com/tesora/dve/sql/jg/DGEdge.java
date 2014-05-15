// OS_STATUS: public
package com.tesora.dve.sql.jg;

public abstract class DGEdge<V extends DGVertex<?>> extends GraphIdentifiable {

	protected V from;
	protected V to;
	
	protected DGEdge(V fromVertex, V toVertex, int id) {
		super(id);
		from = fromVertex;
		to = toVertex;
	}

	public V getFrom() {
		return from;
	}
	
	public V getTo() {
		return to;
	}
	
}
