package com.tesora.dve.sql.schema;

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

import org.apache.commons.lang.ArrayUtils;

import com.tesora.dve.common.catalog.PersistentColumn;
import com.tesora.dve.db.ValueConverter;
import com.tesora.dve.distribution.ColumnDatum;
import com.tesora.dve.distribution.IColumnDatum;
import com.tesora.dve.sql.ConversionException;

public class TColumnDatum implements IColumnDatum {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final PEColumn column;
	private final Object value;
	private String comparatorClassName;
	
	public TColumnDatum(PEColumn col, Object value) {
		column = col;
		this.value = value;
	}

	/*
	@Override
	public String getColumnName() {
		return column.getName().getUnquotedName().get();
	}

	@Override
	public int getColumnId() {
		return column.getPersistentID();
	}
	*/

	@Override
	public Object getValue() {
		if (column.getType().isBinaryType()) {
			if (value instanceof String) {
				return ((String)value).getBytes();
			}
		} else if (column.getType().isNumericType()) {
			try {
				return ValueConverter.INSTANCE.convert(value, column.getType());
			} catch (ConversionException e) {
				// could be conversion of string NULL
				// so just eat the exception and return
				// the value 
			}
		}
		return value;
	}

	@Override
	public int hashCode() {
		Object valueToHash = getValue();
		if (valueToHash != null && valueToHash.getClass().isArray()) {
			return ArrayUtils.hashCode(valueToHash);
		}
		return (valueToHash == null ? 0 : valueToHash.hashCode());
	}

	@Override
	public Comparable<?> getValueForComparison() {
		return ColumnDatum.getValueForComparison(this, value);
	}

	@Override
	public boolean equals(Object o) {
		return ColumnDatum.equals(this,(IColumnDatum)o);
	}

	@Override
	public String getComparatorClassName() {
		return comparatorClassName;
	}
	
	@Override
	public void setComparatorClassName(String comparatorClassName) {
		this.comparatorClassName = comparatorClassName;
	}
	
	@Override
	public String getNativeType() {
		return this.column.getType().getBaseType().getTypeName();
	}
	
	@Override
	public String toString() {
		return column.getId() + "/" + (value == null ? "null" : value.toString());
	}

	@Override
	public PersistentColumn getColumn() {
		return column;
	}
}
