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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public class LogicalInformationSchemaColumn implements Column<LogicalInformationSchemaTable> {

	protected Type type;
	protected LogicalInformationSchemaTable table;
	protected int position = -1;
	protected boolean frozen;	
	protected UnqualifiedName name;
	protected LogicalInformationSchemaTable returnType;
	
	public LogicalInformationSchemaColumn(UnqualifiedName columnName, Type t) {
		type = t;
		this.name = columnName;
		frozen = false;
	}
	
	public void prepare(LogicalInformationSchema schema, DBNative dbn) {
	}
	
	protected final void freeze() {
		frozen = true;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "{name=" + getName() + ", type=" + getType() + "}";
	}

	
	@Override
	public Type getType() {
		return type;
	}

	@Override
	public LogicalInformationSchemaTable getTable() {
		return table;
	}

	@Override
	public void setTable(LogicalInformationSchemaTable t) {
		table = t;
	}

	@Override
	public Name getName() {
		return name;
	}

	public String getFieldName() {
		return null;
	}
		
	public String getColumnName() {
		return null;
	}
	
	public int getDataType() {
		return getType().getBaseType().getDataType();
	}
	
	public int getDataSize() {
		return getType().getSize();
	}
	
	public LogicalInformationSchemaTable getReturnType() {
		return returnType;
	}
	
	public boolean isOptional() {
		return false;
	}
	
	public boolean isID() {
		return false;
	}
	
	public boolean isInjected() {
		return false;
	}
	
	public boolean isBacked() {
		return true;
	}
	
	// by default columns are nullable - the catalog annos will override this
	public boolean isNullable() {
		return true;
	}
	
	public UserColumn persist(CatalogDAO c, UserTable parent, Name nameInView, DBNative dbn) {
		UserColumn uc = dbn.updateUserColumn(null,type);
		uc.setName(nameInView.get());
		uc.setUserTable(parent);
		uc.setHasDefault(Boolean.FALSE);
		uc.setNullable(isNullable());
		return uc;
	}

	public CatalogColumnEntity buildColumnEntity(CatalogSchema schema, CatalogTableEntity parent, int offset, String nameInView) throws PEException {
		CatalogColumnEntity cce = new CatalogColumnEntity(schema, parent);
		cce.setName(nameInView);
		cce.setNullable(isNullable());
		cce.setType(type);
		cce.setPosition(offset);
		return cce;
	}
	
	@Override
	public boolean isTenantColumn() {
		// we may want to overload this to mean - not visible to tenants
		return false;
	}

	@Override
	public int getPosition() {
		return position;
	}

	public void setPosition(int v) {
		position = v;
	}

	// for catalog entities
	public Object getValue(SchemaContext sc, CatalogEntity ce) {
		return ce;
	}
}
