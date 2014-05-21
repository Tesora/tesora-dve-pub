package com.tesora.dve.sql.jg;

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
