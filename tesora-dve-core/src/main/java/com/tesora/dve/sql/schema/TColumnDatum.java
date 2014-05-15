// OS_STATUS: public
package com.tesora.dve.sql.schema;

import org.apache.commons.lang.ArrayUtils;

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
	
	@Override
	public String getColumnName() {
		return column.getName().getUnquotedName().get();
	}

	@Override
	public int getColumnId() {
		return column.getPersistentID();
	}

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
		return getColumnId() + "/" + (value == null ? "null" : value.toString());
	}
}
