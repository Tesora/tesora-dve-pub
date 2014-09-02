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


import java.lang.reflect.Method;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public class CatalogLogicalInformationSchemaColumn extends LogicalInformationSchemaColumn {

	protected InfoSchemaColumn anno;
	
	protected javax.persistence.Column columnAnno;
	protected javax.persistence.JoinColumn joinColumnAnno;

	protected boolean isID;
	
	protected String columnName;
	protected boolean nullable;
	
	// the method that returns the value
	protected Method getter;
	
	protected boolean convertToString;
		
	public CatalogLogicalInformationSchemaColumn(InfoSchemaColumn config, Type t, Method getMethod, String columnName,
			javax.persistence.Column columnAnno, javax.persistence.JoinColumn joinColumnAnno,
			boolean id) {
		super(new UnqualifiedName(config.logicalName()),t);
		this.getter = getMethod;
		this.anno = config;
		this.columnName = columnName;
		this.columnAnno = columnAnno;
		this.joinColumnAnno = joinColumnAnno;
		if (columnAnno != null)
			this.nullable = columnAnno.nullable();
		else if (joinColumnAnno != null)
			this.nullable = joinColumnAnno.nullable();
		this.isID = id;
		convertToString = t.isStringType() && !String.class.equals(getMethod.getReturnType());
	}

	public CatalogLogicalInformationSchemaColumn(CatalogLogicalInformationSchemaColumn given) {
		super(new UnqualifiedName(given.anno.logicalName()),given.getType());
		this.getter = given.getter;
		this.anno = given.anno;
		this.columnName = given.columnName;
		this.convertToString = given.convertToString;
	}
	
	// used for dynamic policies
	protected CatalogLogicalInformationSchemaColumn(UnqualifiedName columnName, Type type) {
		super(columnName, type);
	}
	
	@Override
	public void prepare(LogicalInformationSchema schema,DBNative dbn) {
		if (CatalogEntity.class.isAssignableFrom(this.getter.getReturnType())) {
			for(LogicalInformationSchemaTable tab : schema.getTables(null)) {
				if (tab.getEntityClass() != null && this.getter.getReturnType().isAssignableFrom(tab.getEntityClass())) { 
					returnType = tab;
					break;
				}
			}
			if (returnType == null)
				throw new SchemaException(Pass.FIRST, "Unable to resolve logical schema table for column " + getName() + " return type");
		}
	}

	@Override
	public String toString() {
		return "CatalogInformationSchemaColumn{name=" + getName() + ", type=" + getType() + ", fieldName=" + getFieldName() + ", columnName=" + columnName + "}";
	}

	@Override
	public String getFieldName() {
		return this.anno.fieldName();
	}
	
	@Override
	public String getColumnName() {
		return this.columnName;
	}
	
	@Override
	public boolean isID() {
		return isID;
	}
	
	@Override
	public boolean isInjected() {
		return this.anno.injected();
	}
	
	// for raw entity queries
	protected Object getRawValue(SchemaContext sc, CatalogEntity ce) {
		try {
			return getter.invoke(ce, new Object[] {});
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, "Unable to obtain value for " + getName().getSQL() + " from object of type " + ce.getClass().getSimpleName(),t);
		}		
	}
	
	// for raw entity queries
	@Override
	public Object getValue(SchemaContext sc, CatalogEntity ce) {
		if (ce == null) return null;
		
		Object r = getRawValue(sc,ce);
		if (r == null) return null;
		if (convertToString) {
			if (r instanceof Boolean) {
				return (Boolean.TRUE.equals(r) ? this.anno.booleanStringTrueFalseValue()[0] : this.anno.booleanStringTrueFalseValue()[1]);
			} else if (r instanceof CatalogEntity) {
				return r;
			} else if (r instanceof Number) {
				return r.toString();
			} else {
				throw new SchemaException(Pass.PLANNER, "What is show rep for " + r);
			}
		}
		return r;
	}
	
	public LogicalInformationSchemaTable getReturnTable() {
		return returnType;
	}
	
	@Override
	public boolean isBacked() {
		return columnName != null;
	}
	
	@Override
	public boolean isNullable() {
		return nullable;
	}

}
