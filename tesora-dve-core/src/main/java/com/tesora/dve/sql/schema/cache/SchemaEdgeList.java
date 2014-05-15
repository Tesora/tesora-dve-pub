// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import java.util.ArrayList;
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
}
