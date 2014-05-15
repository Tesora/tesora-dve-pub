// OS_STATUS: public
package com.tesora.dve.distribution;

import java.io.Serializable;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.ArrayUtils;

import com.tesora.dve.common.catalog.PersistentColumn;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.sql.schema.types.BasicType;

public class ColumnDatum implements Serializable, IColumnDatum {
	
	private static final long serialVersionUID = 1L;
	
	int columnId;
	
	transient PersistentColumn userColumn;
	
	Object value = null;
	
	String comparatorClassName;

	public ColumnDatum(PersistentColumn col) {
		columnId = col.getId();
		userColumn = col;
	}

	public ColumnDatum(PersistentColumn col, Object v) {
		this(col);
		value = v;
	}
	
	public ColumnDatum(ColumnDatum other) {
		this.columnId = other.columnId;
		this.value = other.value;
		this.userColumn = other.userColumn;
	}

	public PersistentColumn getUserColumn() {
		return userColumn;
	}

	@Override
	public String getColumnName() {
		return userColumn.getPersistentName();
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
						(String)value, BasicType.getDateTimeType());
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
		return left.getColumnId() == right.getColumnId()
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
		return columnId + "/" + (value == null ? "null" : value.toString());
	}

	@Override
	public int getColumnId() {
		return columnId;
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
		return this.userColumn.getNativeTypeName();
	}
}
