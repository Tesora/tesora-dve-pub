// OS_STATUS: public
package com.tesora.dve.sql.schema.types;

import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.mysql.MysqlNativeType;

public class SizedType extends BasicType {

	protected int size;
	
	protected SizedType(NativeType nt, short flags, int size) {
		super(nt,flags);
		this.size = size;
	}
	
	@Override
	public int getSize() {
		return size;
	}
	
	@Override
	public boolean hasSize() {
		return getSize() > 0;
	}
	
	@Override
	public boolean declUsesSizing() {
		return true;
	}

	@Override
	public Integer getIndexSize() {
		MysqlNativeType mnt = (MysqlNativeType) getBaseType();
		if (mnt == null) return null;
		switch(mnt.getMysqlType()) {
		case BIT:
		{
			return (getSize() + 7)/8;
		}
		default:
			return super.getIndexSize();
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + size;
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
		SizedType other = (SizedType) obj;
		if (size != other.size)
			return false;
		return true;
	}
	
	@Override
	public boolean isAcceptableColumnTypeForRangeType(Type columnType) {
		if (columnType instanceof SizedType) {
			SizedType ost = (SizedType) columnType;
			if ((isIntegralType() || isBitType()) && (ost.isIntegralType() || ost.isBitType())) {
				// first look at storage size.  if the column storage size is smaller than the range storage size - automagically works.
				int rss = getStorageSize(getMysqlType().getMysqlType());
				int css = getStorageSize(columnType.getMysqlType().getMysqlType());
				if (rss == -1 || css == -1)
					// something is unknown - be safe and say no
					return false;
				if (css > rss)
					// column type is larger
					return false;
				if (isBitType() && ost.isBitType()) {
					// size matters
					return getSize() >= ost.getSize();
				}
				// column type is smaller or equal
				return true;
			}
		}
		// generally speaking - for everything that derives (text types, etc.) - the default comparison is fine
		// because we don't support using a varchar(8) column on a varchar(16) range - no padding is done
		// and likewise for other text types, varbinary, etc.
		return super.isAcceptableColumnTypeForRangeType(columnType);
	}

	@Override
	public boolean isAcceptableRangeType() {
		if (!super.isAcceptableRangeType())
			return false;
		return !isFloatType();
	}


	public static int getStorageSize(MysqlNativeType.MysqlType type) {
		switch(type) {
		case TINYINT:
			return 1;
		case BIGINT:
			return 8;
		case INT:
			return 4;
		case MEDIUMINT:
			return 3;
		case SMALLINT:
			return 2;
		case BIT:
			return 0;
		default:
			return -1;
		}
	}


}
