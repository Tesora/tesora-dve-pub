// OS_STATUS: public
package com.tesora.dve.sql.schema.types;

import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.mysql.MysqlNativeType;


public class FloatingPointType extends SizedType {

	protected int scale;
	protected int precision;
	
	protected FloatingPointType(NativeType nt, short flags, int length, int precision, int scale) {
		super(nt,flags,length);
		this.scale = scale;
		this.precision = precision;
	}
	
	@Override
	public boolean hasPrecisionAndScale() {
		return true;
	}
	
	@Override
	public int getPrecision() {
		return precision;
	}
	
	@Override
	public int getScale() {
		return scale;
	}

	@Override
	public Integer getIndexSize() {
		MysqlNativeType mnt = (MysqlNativeType) getBaseType();
		switch(mnt.getMysqlType()) {
		case DECIMAL:
		{
			int lhs = getPrecision() - getScale();
			return computeBytes(lhs) + computeBytes(getScale());
		}
		default:
			return super.getIndexSize();
		}
	}
	
	private static final int computeBytes(int digits) {
		return digits / 9 + leftovers[digits % 9];
	}
	
	private static final int[] leftovers = 
			new int[] { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + precision;
		result = prime * result + scale;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		FloatingPointType other = (FloatingPointType) obj;
		if (precision != other.precision)
			return false;
		if (scale != other.scale)
			return false;
		return true;
	}	
	
	@Override
	public boolean isAcceptableColumnTypeForRangeType(Type columnType) {
		// floats/decimals are never acceptable
		return false;
	}

}
