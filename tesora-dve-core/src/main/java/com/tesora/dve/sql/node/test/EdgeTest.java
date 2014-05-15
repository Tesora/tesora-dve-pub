// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import java.util.List;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.IEdgeName;
import com.tesora.dve.sql.node.LanguageNode;

public class EdgeTest implements NodeTest<LanguageNode> {

	protected IEdgeName name;
	
	public EdgeTest(IEdgeName name) {
		this.name = name;
	}
	
	public IEdgeName getName() {
		return name;
	}
	
	public Edge<?, LanguageNode> getEdge(LanguageNode in) {
		return in.getAttribute(name);
	}
	
	@Override
	public boolean has(LanguageNode in) {
		Edge<?, LanguageNode> edge = getEdge(in);
		if (edge == null) return false;
		return edge.has();
	}

	@Override
	public boolean has(LanguageNode in, EngineToken tok) {
		return has(in);
	}

	@Override
	public LanguageNode get(LanguageNode in) {
		Edge<?, LanguageNode> edge = getEdge(in);
		if (edge == null) return null;
		return edge.get();
	}

	@Override
	public LanguageNode get(LanguageNode in, EngineToken tok) {
		return get(in);
	}

	@Override
	public List<LanguageNode> getMulti(LanguageNode in) {
		Edge<?, LanguageNode> edge = getEdge(in);
		if (edge == null) return null;
		return edge.getMulti();
	}
	
	@Override
	public String toString() {
		return getName().toString();
	}

}
