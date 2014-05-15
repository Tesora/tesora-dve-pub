// OS_STATUS: public
package com.tesora.dve.sql.node;

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
