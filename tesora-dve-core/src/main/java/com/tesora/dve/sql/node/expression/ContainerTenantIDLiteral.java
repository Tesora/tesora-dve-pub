// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.PEContainerTenant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.transform.CopyContext;

public class ContainerTenantIDLiteral extends TenantIDLiteral {

	private SchemaCacheKey<PEContainer> container;
	
	public ContainerTenantIDLiteral(ValueSource vs, SchemaCacheKey<PEContainer> spec) {
		super(vs);
		container = spec;
	}
	
	protected ContainerTenantIDLiteral(ContainerTenantIDLiteral other) {
		super(other);
		this.container = other.container;
	}

	protected static void validate(SchemaCacheKey<PEContainer> expecting, SchemaContext sc) {
		SchemaEdge<IPETenant> e = sc.getCurrentTenant();
		IPETenant actual = e.get(sc);
		if (actual == null)
			throw new SchemaException(Pass.PLANNER, "Require a tenant");
		if (actual instanceof PEContainerTenant) {
			PEContainerTenant pect = (PEContainerTenant) actual;
			SchemaCacheKey<PEContainer> currentContainer = pect.getContainerCacheKey();
			if (currentContainer == null)
				throw new SchemaException(Pass.PLANNER, "Require a container tenant");
			else if (!currentContainer.equals(expecting))
				throw new SchemaException(Pass.PLANNER, "Invalid container context");
		} else {
			throw new SchemaException(Pass.PLANNER, "Invalid current tenant for plan - expected container tenant but found " + actual.getClass().getSimpleName());
		}		
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		validate(container,sc);
		return source.getTenantID(sc);
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		TenantIDLiteral out = new TenantIDLiteral(this);
		return out;
	}

	@Override
	public ILiteralExpression getCacheExpression() {
		return new CachedContainerTenantIDLiteral(getValueType(),container);
	}


}
