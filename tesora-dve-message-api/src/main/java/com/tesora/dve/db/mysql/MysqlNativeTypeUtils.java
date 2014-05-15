// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.resultset.ColumnMetadata;

public final class MysqlNativeTypeUtils {

	private MysqlNativeTypeUtils() {
		// no public constructor
	}

	public static boolean isUnsigned(ColumnMetadata columnMetadata, MysqlNativeType mysqlNativeType) {
		// hack to mimic Mysql behaviour
		if (MysqlType.BIT.equals(mysqlNativeType.getMysqlType()))
				return true;
		
		return mysqlNativeType.isUnsignedAttribute() && isUnsigned(columnMetadata);
	}

	public static boolean isUnsigned(ColumnMetadata columnMetadata) {
		return columnMetadata != null && columnMetadata.getNativeTypeModifiers() != null &&
				columnMetadata.getNativeTypeModifiers().contains(MysqlNativeType.MODIFIER_UNSIGNED);
	}
}
