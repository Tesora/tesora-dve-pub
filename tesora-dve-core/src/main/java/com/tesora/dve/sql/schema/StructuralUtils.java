// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.cache.TransientEdge;

public final class StructuralUtils {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static SchemaEdge buildEdge(SchemaContext sc, Object p, boolean persistent) {
		if (sc == null)
			return new TransientEdge(p);
		if (persistent)
			return sc.getSource().buildEdge(p);
		else
			return sc.getSource().buildTransientEdge(p);
	}
	
}
