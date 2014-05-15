// OS_STATUS: public
package com.tesora.dve.sql.jg;

import java.util.ArrayList;
import java.util.List;

public abstract class DGVertex<E extends DGEdge<?>> extends GraphIdentifiable {

	protected List<E> edges;
	
	protected DGVertex(int gid) {
		super(gid);
		edges = new ArrayList<E>();
	}
	
	public List<E> getEdges() {
		return edges;
	}
	
	public void addEdge(E e) {
		edges.add(e);
	}
	
}
