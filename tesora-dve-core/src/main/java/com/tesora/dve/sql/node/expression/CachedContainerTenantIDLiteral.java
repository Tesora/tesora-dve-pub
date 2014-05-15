// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;

public class CachedContainerTenantIDLiteral extends CachedTenantIDLiteral {

	private SchemaCacheKey<PEContainer> container;
	
	public CachedContainerTenantIDLiteral(int type, SchemaCacheKey<PEContainer> cont) {
		super(type);
		container = cont;
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		ContainerTenantIDLiteral.validate(container,sc);
		return sc.getValueManager().getTenantID(sc);
	}

}
