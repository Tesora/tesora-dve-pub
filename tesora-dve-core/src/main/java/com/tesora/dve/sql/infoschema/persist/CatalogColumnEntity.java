package com.tesora.dve.sql.infoschema.persist;

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

import com.tesora.dve.db.mysql.common.ColumnAttributes;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.SimplePersistedEntity;
import com.tesora.dve.sql.schema.types.Type;

public class CatalogColumnEntity extends SimplePersistedEntity {

	public CatalogColumnEntity(CatalogSchema cs, CatalogTableEntity cte) throws PEException {
		super(cs.getColumn());
		addRequires(cte);
		preValue("flags",ColumnAttributes.set(0, ColumnAttributes.CONSTANT_DEFAULT_VALUE));
		preValue("cdv",0);
		preValue("hash_position",0);
	}
	
	public void setName(String n) throws PEException {
		preValue("name",n);
	}
	
	public int getFlags() {
		return (Integer)getValue("flags");
	}

	public void setFlags(int v) throws PEException {
		preValue("flags",v);
	}
	
	public void setNullable(boolean v) throws PEException {
		preValue("flags",
				(v ? ColumnAttributes.clear(getFlags(), ColumnAttributes.NOT_NULLABLE)
				   : ColumnAttributes.set(getFlags(), ColumnAttributes.NOT_NULLABLE)));
	}
	
	public void setPosition(int v) throws PEException {
		preValue("order_in_table",v);
	}
	
	public void setNativeTypeModifiers(String v) throws PEException {
		preValue("native_type_modifiers",v);		
	}
	
	public String getNativeTypeModifiers() {
		return (String)getValue("native_type_modifiers"); 
	}
	
	public void setCharset(String v) throws PEException {
		preValue("charset",v);
	}
	
	public void setCollation(String v) throws PEException {
		preValue("collation",v);
	}
	
	public String getCharset() { 
		return (String)getValue("charset");
	}
	
	public String getCollation() {
		return (String)getValue("collation");
	}
	
	public void setType(Type t) throws PEException {
		preValue("native_type_name",t.getTypeName());
		preValue("data_type",t.getDataType());
		if (t.hasSize()) {
			preValue("size",t.getSize());
			if (t.hasPrecisionAndScale()) {
				preValue("prec",t.getPrecision());
				preValue("scale",t.getScale());
			} else {
				preValue("prec",0);
				preValue("scale",0);
			}
		} else {
			preValue("size",0);
			preValue("prec",0);
			preValue("scale",0);
		}
		t.addColumnTypeModifiers(this);
	}
}
