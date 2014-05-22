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
