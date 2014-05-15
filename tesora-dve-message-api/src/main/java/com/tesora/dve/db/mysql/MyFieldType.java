// OS_STATUS: public
package com.tesora.dve.db.mysql;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.exceptions.PEException;

/**
 * Enum to map Mysql Native Types to the Mysql wire protocol types used in Field Packets
 * 
 */
public enum MyFieldType {
	
	FIELD_TYPE_DECIMAL((byte) 0x00),
	FIELD_TYPE_TINY((byte) 0x01), 
	FIELD_TYPE_SHORT((byte) 0x02),
	FIELD_TYPE_LONG((byte) 0x03), 
	FIELD_TYPE_FLOAT((byte) 0x04), 
	FIELD_TYPE_DOUBLE((byte) 0x05),
	FIELD_TYPE_NULL((byte) 0x06), 
	FIELD_TYPE_TIMESTAMP((byte) 0x07), 
	FIELD_TYPE_LONGLONG((byte) 0x08), 
	FIELD_TYPE_INT24((byte) 0x09), 
	FIELD_TYPE_DATE((byte) 0x0a),
	FIELD_TYPE_TIME((byte) 0x0b), 
	FIELD_TYPE_DATETIME((byte) 0x0c),
	FIELD_TYPE_YEAR((byte) 0x0d),
	FIELD_TYPE_NEWDATE((byte) 0x0e), 
	FIELD_TYPE_VARCHAR((byte) 0x0f), 
	FIELD_TYPE_BIT((byte) 0x10),
	FIELD_TYPE_NEWDECIMAL((byte) 0xf6), 
	FIELD_TYPE_ENUM((byte) 0xf7), 
	FIELD_TYPE_SET((byte) 0xf8),
	FIELD_TYPE_TINY_BLOB((byte) 0xf9), 
	FIELD_TYPE_MEDIUM_BLOB((byte) 0xfa), 
	FIELD_TYPE_LONG_BLOB((byte) 0xfb), 
	FIELD_TYPE_BLOB((byte) 0xfc), 
	FIELD_TYPE_VAR_STRING((byte) 0xfd),
	FIELD_TYPE_STRING((byte) 0xfe), 
	FIELD_TYPE_GEOMETRY((byte) 0xff);

	static Map<Byte, MyFieldType> valueMap = new HashMap<Byte, MyFieldType>();
	static {
		for( MyFieldType mft : values() ) {
			valueMap.put(new Byte(mft.getByteValue()), mft);
		}
	}

	private final byte fieldTypeasByte;

	private MyFieldType(byte b) {
		fieldTypeasByte = b;
	}

	public boolean isBinaryFlagDependent() {
		return FIELD_TYPE_BLOB.equals(this)
				|| FIELD_TYPE_VAR_STRING.equals(this)
				|| FIELD_TYPE_STRING.equals(this);
	}
	
	public boolean supportsUnsigned() {
		return FIELD_TYPE_DECIMAL.equals(this)
				|| FIELD_TYPE_DOUBLE.equals(this)
				|| FIELD_TYPE_FLOAT.equals(this)
				|| FIELD_TYPE_INT24.equals(this)
				|| FIELD_TYPE_LONG.equals(this)
				|| FIELD_TYPE_LONGLONG.equals(this)
				|| FIELD_TYPE_NEWDECIMAL.equals(this)
				|| FIELD_TYPE_SHORT.equals(this)
				|| FIELD_TYPE_TINY.equals(this);

	}
	
	public static MyFieldType fromByte(byte b)	{
		return valueMap.get(b);
	}
	
	public byte getByteValue() {
		return fieldTypeasByte;
	}
	
	public static MyFieldType mapFromNativeType(String name) throws PEException {
		return mapFromNativeType(MysqlType.toMysqlType(name));
	}
	
	public static MyFieldType mapFromNativeType(MysqlType mysqlType) throws PEException {

		switch (mysqlType) { // NOPMD by doug on 30/11/12 4:36 PM
		case BIGINT:
			return FIELD_TYPE_LONGLONG;
		case BINARY:
			return FIELD_TYPE_STRING;
		case BIT:
		case BOOL:
			return FIELD_TYPE_BIT;
		case BLOB:
//		case LONG_VARBINARY:
		case LONGBLOB:
		case MEDIUMBLOB:
		case TINYBLOB:
		case VARCHAR:
		case LONGTEXT:
		case MEDIUMTEXT:
		case TINYTEXT:
		case TEXT:
			if ( mysqlType.getSqlType() == Types.VARCHAR )
				return FIELD_TYPE_VAR_STRING;
			return FIELD_TYPE_BLOB;
		case CHAR:
//		case NCHAR:
			return FIELD_TYPE_STRING;
		case DATE:
			return FIELD_TYPE_DATE;
		case DATETIME:
			return FIELD_TYPE_DATETIME;
		case DECIMAL:
//		case NUMERIC:
			return FIELD_TYPE_NEWDECIMAL;
		case DOUBLE:
		case DOUBLE_PRECISION:
//		case REAL: // we don't use REAL in mysql, it maps to DOUBLE
			return FIELD_TYPE_DOUBLE;
		case ENUM:
			return FIELD_TYPE_ENUM;
		case FLOAT:
			return FIELD_TYPE_FLOAT;
		case INT:
//		case INTEGER:
			return FIELD_TYPE_LONG;
//		case LONG_NVARCHAR:
//		case LONG_VARCHAR:
//		case NVARCHAR:
//			return FIELD_TYPE_VARCHAR;
		case VARBINARY:
			return FIELD_TYPE_VAR_STRING;
		case MEDIUMINT:
			return FIELD_TYPE_INT24;
		case SMALLINT:
			return FIELD_TYPE_SHORT;
		case SET:
			return FIELD_TYPE_SET;
		case TIME:
			return FIELD_TYPE_TIME;
		case TIMESTAMP:
			return FIELD_TYPE_TIMESTAMP;
		case TINYINT:
			return FIELD_TYPE_TINY;
		case NULL:
			return FIELD_TYPE_NULL;
		case PARAMETER:
			return FIELD_TYPE_VAR_STRING;
		default:
			throw new PEException("Unknown mysql type: '" + mysqlType + "'");
		}
	}
}
