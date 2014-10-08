package com.tesora.dve.distribution;

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

import java.io.Serializable;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.ArrayUtils;

import com.tesora.dve.common.catalog.PersistentColumn;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.sql.schema.types.BasicType;

public class ColumnDatum implements Serializable, IColumnDatum {
	
	private static final long serialVersionUID = 1L;
	
	
	transient PersistentColumn userColumn;
	
	Object value = null;
	
	String comparatorClassName;

	public ColumnDatum(PersistentColumn col) {
		userColumn = col;
	}

	public ColumnDatum(PersistentColumn col, Object v) {
		this(col);
		value = v;
	}
	
	public ColumnDatum(ColumnDatum other) {
		this.value = other.value;
		this.userColumn = other.userColumn;
	}

	public PersistentColumn getUserColumn() {
		return userColumn;
	}

	public void setValue(Object object) {
		value = object;
	}
	
	@Override
	public Object getValue() {
		return value;
	}
	
	@Override
	public Comparable<?> getValueForComparison() {
		return getValueForComparison(this, value);
	}

	public static Comparable<?> getValueForComparison(IColumnDatum col, Object value) {
		Comparable<?> valueForCompare = (Comparable<?>) value;
		if (MysqlNativeType.MysqlType.DATETIME.toString().equals(col.getNativeType())) {
			if (value instanceof String) {
                valueForCompare = (Comparable<?>) Singletons.require(HostService.class).getDBNative().getValueConverter().convert(
						(String)value, BasicType.getDateTimeType(Singletons.require(HostService.class).getDBNative().getTypeCatalog()));
			}
		}
		else
		{
			if (value instanceof Integer)
				valueForCompare = new Long(Long.valueOf(((Integer) value).longValue()));
			else if (value instanceof Short)
				valueForCompare = new Long(Long.valueOf(((Short) value).longValue()));
			else if (value instanceof Byte)
				valueForCompare = new Long(Long.valueOf(((Byte) value).longValue()));
		}
		return valueForCompare;
	}

	@Override
	public boolean equals(Object o) {
		return equals(this,(IColumnDatum)o);
	}

	public static boolean equals(IColumnDatum left, IColumnDatum right) {
		return left.getColumn().equals(right.getColumn())
			&& ((left.getValue() == null && right.getValue() == null) || left.getValue().equals(right.getValue()));
	}
	
	@Override
	public int hashCode() {
		if (value != null && value.getClass().isArray()) {
			return ArrayUtils.hashCode(value);
		}
		return (value == null ? 0 : value.hashCode());
	}
	
	@Override
	public String toString() {
		return getColumn() + "/" + (value == null ? "null" : value.toString());
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
		return this.userColumn.getTypeName();
	}

	@Override
	public PersistentColumn getColumn() {
		return userColumn;
	}
}
