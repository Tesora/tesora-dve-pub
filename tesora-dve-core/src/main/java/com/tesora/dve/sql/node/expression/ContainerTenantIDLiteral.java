package com.tesora.dve.sql.node.expression;

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

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.PEContainerTenant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.transform.CopyContext;

public class ContainerTenantIDLiteral extends TenantIDLiteral {

	public ContainerTenantIDLiteral(ValueSource vs) {
		super(vs);
	}
	
	protected ContainerTenantIDLiteral(ContainerTenantIDLiteral other) {
		super(other);
	}

	@Override
	public Object getValue(ConnectionValues cv) {
		if (source != null) return source.getTenantID();
		return cv.getTenantID();
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		TenantIDLiteral out = new TenantIDLiteral(this);
		return out;
	}

	@Override
	public ILiteralExpression getCacheExpression() {
		return new CachedContainerTenantIDLiteral(getValueType());
	}


}
