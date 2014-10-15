package com.tesora.dve.sql.schema.cache;

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
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.StructuralUtils;

public class SchemaEdgeList<TClass> extends ArrayList<SchemaEdge<TClass>> {

	private static final long serialVersionUID = 1L;

	public SchemaEdgeList() {
		super();
	}
	
	public List<TClass> resolve(SchemaContext sc) {
		ArrayList<TClass> out = new ArrayList<TClass>();
		for(SchemaEdge<TClass> i : this) {
			out.add(i.get(sc));
		}
		return out;
	}
	
	@SuppressWarnings("unchecked")
	public void add(SchemaContext sc, TClass o, boolean persistent) {
		add(StructuralUtils.buildEdge(sc,o, persistent));
	}
	
	public void remove(SchemaContext sc, TClass o) {
		for(Iterator<SchemaEdge<TClass>> iter = iterator(); iter.hasNext();) {
			if (iter.next().get(sc) == o)
				iter.remove();
		}
	}
}
