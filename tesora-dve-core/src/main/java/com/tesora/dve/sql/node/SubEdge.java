package com.tesora.dve.sql.node;

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

public class SubEdge<O extends LanguageNode, T extends LanguageNode> extends SingleEdge<O,T> {

	public SubEdge(Class<O> oc, O owner, OffsetEdgeName oen) {
		super(oc, owner, oen);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public LanguageNode apply(LanguageNode in) {
		List<Edge<?,?>> edges = in.getEdges();
		OffsetEdgeName myname = (OffsetEdgeName) this.getName();
		for(Edge<?,?> e : edges) {
			if (myname.baseMatches(e.getName())) {
				// e is a multiedge, get the ith item
				MultiEdge me = (MultiEdge) e;
				if (myname.offset < me.size())
					return me.get(myname.offset);
				else 
					throw new MigrationException("Edge name " + e.getName() + " matches " + this.getName() + " but the collection is too small (" + me.size() + ")");
			}
		}
		throw new MigrationException("No such edge name in " + in + ": " + this.getName());
	}

}
