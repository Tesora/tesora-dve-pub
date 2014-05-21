package com.tesora.dve.sql.infoschema;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public class SyntheticInformationSchemaColumn extends
		AbstractInformationSchemaColumnView {

	private Type type;
	
	public SyntheticInformationSchemaColumn(InfoView view, UnqualifiedName nameInView, Type type) {
		super(view,nameInView);
		this.type = type;
	}
	
	public SyntheticInformationSchemaColumn(UnqualifiedName nameInView) {
		this(null,nameInView,null);
	}

	@Override
	public Type getType() {
		return type;
	}

	@Override
	public LogicalInformationSchemaColumn getLogicalColumn() {
		return null;
	}

	@Override
	public boolean isSynthetic() {
		return true;
	}
	
	@Override
	public void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte, int offset, List<PersistedEntity> acc) throws PEException {
		if (type == null) return;
		CatalogColumnEntity cce = new CatalogColumnEntity(schema,cte);
		cce.setName(getName().get());
		cce.setNullable(Boolean.TRUE);
		cce.setType(type);
		cce.setPosition(offset);
		acc.add(cce);
	}

	@Override
	public AbstractInformationSchemaColumnView copy() {
		return new SyntheticInformationSchemaColumn(view,name,type);
	}
	
	public ExpressionNode buildReplacement(ColumnInstance subject) {
		return subject;
	}

}
