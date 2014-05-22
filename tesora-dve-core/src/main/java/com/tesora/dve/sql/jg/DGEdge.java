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
