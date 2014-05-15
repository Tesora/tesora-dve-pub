// OS_STATUS: public
package com.tesora.dve.db.mysql;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.NativeTypeAlias;
import com.tesora.dve.exceptions.PEException;

public class MysqlNativeType extends NativeType {

	public static final String MODIFIER_UNSIGNED = "unsigned";
	public static final String MODIFIER_SIGNED = "signed";
	public static final String MODIFIER_ZEROFILL = "zerofill";

	private static final long serialVersionUID = 1L;

	private final MysqlType mysqlType;
	private byte charSet;
	private short fieldTypeFlags; // used by PEMysqlProtocolConverter


	/**
	 * Enum representing all of the MySQL native types. The toMysqlType method can be used to convert a string
	 * representing a MySQL type into this enumeration
	 * 
	 */
	public enum MysqlType {
		NULL(MyFieldType.FIELD_TYPE_NULL, Types.NULL, false),
//		LONG_NVARCHAR(MyFieldType.FIELD_TYPE_BLOB, Types.LONGNVARCHAR, false),
//		NCHAR(MyFieldType.FIELD_TYPE_STRING, Types.NCHAR, false),
//		NVARCHAR(MyFieldType.FIELD_TYPE_VAR_STRING, Types.NVARCHAR, false),
		BIT(MyFieldType.FIELD_TYPE_BIT, Types.BIT),
		BOOL(MyFieldType.FIELD_TYPE_TINY, Types.BIT, new NativeTypeAlias[] { new NativeTypeAlias("BOOLEAN") }),
		TINYINT(MyFieldType.FIELD_TYPE_TINY, Types.TINYINT, new NativeTypeAlias[] { new NativeTypeAlias("INT1"),
				new NativeTypeAlias("TINYINT UNSIGNED", true) }),
		BIGINT(MyFieldType.FIELD_TYPE_LONGLONG, Types.BIGINT, new NativeTypeAlias[] { new NativeTypeAlias("INT8"),
				new NativeTypeAlias("BIGINT UNSIGNED", true) }),
		//LONG_VARBINARY(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARBINARY),
		MEDIUMBLOB(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARBINARY,
				new NativeTypeAlias[] { new NativeTypeAlias("LONG_VARBINARY") }),
		LONGBLOB(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARBINARY),
		// DAS - these were added because the defintion of LONG/MEDIUM/TINY BLOB seems to change if it is in
		//       a column defintion vs a transient column (compare col1 longblob vs select @sql_mode)
		ALTLONGBLOB(MyFieldType.FIELD_TYPE_LONG_BLOB, Types.LONGVARBINARY),
		ALTMEDIUMBLOB(MyFieldType.FIELD_TYPE_MEDIUM_BLOB, Types.LONGVARBINARY),
		ALTTINYBLOB(MyFieldType.FIELD_TYPE_TINY_BLOB, Types.LONGVARBINARY),
		//
		BLOB(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARBINARY),
		TINYBLOB(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARBINARY),
		VARBINARY(MyFieldType.FIELD_TYPE_VAR_STRING, Types.VARBINARY),
		BINARY(MyFieldType.FIELD_TYPE_STRING, Types.BINARY),
		//LONG_VARCHAR(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARCHAR),
		MEDIUMTEXT(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARCHAR,
				new NativeTypeAlias[] { new NativeTypeAlias("LONG_VARCHAR") }),
		LONGTEXT(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARCHAR),
		TEXT(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARCHAR),
		TINYTEXT(MyFieldType.FIELD_TYPE_BLOB, Types.LONGVARCHAR),
		CHAR(MyFieldType.FIELD_TYPE_STRING, Types.CHAR, new NativeTypeAlias[] { new NativeTypeAlias("CHARACTER"), new NativeTypeAlias("NCHAR") }),
//		NUMERIC(MyFieldType.FIELD_TYPE_NEWDECIMAL, Types.NUMERIC),
		DECIMAL(MyFieldType.FIELD_TYPE_NEWDECIMAL, Types.DECIMAL, new NativeTypeAlias[] { new NativeTypeAlias("DEC"), new NativeTypeAlias("FIXED"),
				new NativeTypeAlias("NUMERIC") }),
//		INT(MyFieldType.FIELD_TYPE_LONG, Types.INTEGER, new NativeTypeAlias[] { new NativeTypeAlias("INT UNSIGNED", true) }),
//		INTEGER(MyFieldType.FIELD_TYPE_LONG, Types.INTEGER, new NativeTypeAlias[] { new NativeTypeAlias("INT4"),
//				new NativeTypeAlias("INTEGER UNSIGNED", true) }),
		INT(MyFieldType.FIELD_TYPE_LONG, Types.INTEGER, new NativeTypeAlias[] { new NativeTypeAlias("INT UNSIGNED", true),
				new NativeTypeAlias("INT4"), new NativeTypeAlias("INTEGER", true), new NativeTypeAlias("INTEGER UNSIGNED", true) }),
		MEDIUMINT(MyFieldType.FIELD_TYPE_INT24, Types.INTEGER,
				new NativeTypeAlias[] { new NativeTypeAlias("MEDIUMINT UNSIGNED", true), new NativeTypeAlias("INT3"), new NativeTypeAlias("MIDDLEINT") }),
		SMALLINT(MyFieldType.FIELD_TYPE_SHORT, Types.SMALLINT, new NativeTypeAlias[] { new NativeTypeAlias("INT2"),
				new NativeTypeAlias("SMALLINT UNSIGNED", true) }),
		FLOAT(MyFieldType.FIELD_TYPE_FLOAT, Types.REAL),
		DOUBLE(MyFieldType.FIELD_TYPE_DOUBLE, Types.DOUBLE, new NativeTypeAlias[] { new NativeTypeAlias("FLOAT8") }),
		DOUBLE_PRECISION(MyFieldType.FIELD_TYPE_DOUBLE, Types.DOUBLE, new NativeTypeAlias[] { new NativeTypeAlias("REAL", true) }),
		VARCHAR(MyFieldType.FIELD_TYPE_VAR_STRING, Types.VARCHAR, new NativeTypeAlias[] { new NativeTypeAlias("CHARACTER VARYING"),
				new NativeTypeAlias("VARCHARACTER"), new NativeTypeAlias("NVARCHAR") }),
		DATE(MyFieldType.FIELD_TYPE_DATE, Types.DATE),
		TIME(MyFieldType.FIELD_TYPE_TIME, Types.TIME),
		TIMESTAMP(MyFieldType.FIELD_TYPE_TIMESTAMP, Types.TIMESTAMP),
		DATETIME(MyFieldType.FIELD_TYPE_DATETIME, Types.TIMESTAMP),
		YEAR(MyFieldType.FIELD_TYPE_YEAR, Types.DATE, false),
		PARAMETER(MyFieldType.FIELD_TYPE_VAR_STRING, Types.VARCHAR, false),
		ENUM(MyFieldType.FIELD_TYPE_ENUM, Types.VARCHAR),
		SET(MyFieldType.FIELD_TYPE_SET, Types.VARCHAR),
		// REAL(Types.DOUBLE), // this is now an alias of DOUBLE
		// REAL_UNUSED(MyFieldType.FIELD_TYPE_FLOAT, Types.FLOAT, false),

		/* Spatial types. */
		GEOMETRY(MyFieldType.FIELD_TYPE_GEOMETRY, Types.BINARY),
		POINT(MyFieldType.FIELD_TYPE_GEOMETRY, Types.BINARY),
		LINESTRING(MyFieldType.FIELD_TYPE_GEOMETRY, Types.BINARY),
		POLYGON(MyFieldType.FIELD_TYPE_GEOMETRY, Types.BINARY),
		GEOMETRYCOLLECTION(MyFieldType.FIELD_TYPE_GEOMETRY, Types.BINARY),
		MULTIPOINT(MyFieldType.FIELD_TYPE_GEOMETRY, Types.BINARY),
		MULTILINESTRING(MyFieldType.FIELD_TYPE_GEOMETRY, Types.BINARY),
		MULTIPOLYGON(MyFieldType.FIELD_TYPE_GEOMETRY, Types.BINARY),

		UNKNOWN(MyFieldType.FIELD_TYPE_NULL, Types.NULL, false);

		private MysqlType(MyFieldType mft, int sqlType) {
			this(mft, sqlType, true, null);
		}

		private MysqlType(MyFieldType mft, int sqlType, boolean jdbcType) {
			this(mft, sqlType, jdbcType, null);
		}

		private MysqlType(MyFieldType mft, int sqlType, NativeTypeAlias[] aliases) {
			this(mft, sqlType, true, aliases);
		}

		private MysqlType(MyFieldType mft, int sqlType, boolean jdbcType, NativeTypeAlias[] aliases) {
			this.name = NativeType.fixName(super.toString(), true);
			this.mft = mft;
			this.sqlType = sqlType;
			this.jdbcType = jdbcType;
			this.aliases = aliases;
			this.isValid = this.toString().equalsIgnoreCase("UNKNOWN") ? false : true;
		}

		private final String name;
		private final MyFieldType mft;
		private final int sqlType;
		private final boolean jdbcType;
		private final NativeTypeAlias[] aliases;
		private final boolean isValid;
		private static MysqlNativeTypeCatalog mntCatalog = null;

		public static MysqlType toMysqlType(String name) {
			String fixedName = NativeType.fixNameForType(name);
			try {
				return valueOf(fixedName);
			} catch (IllegalArgumentException iae) {
				// maybe it's an alias
				try {
					if (mntCatalog == null) {
						mntCatalog = new MysqlNativeTypeCatalog();
						mntCatalog.load();
					}
					return valueOf(NativeType.fixNameForType(
							((MysqlNativeType) mntCatalog.findType(fixedName, true)).getMysqlType().toString()));
				} catch (IllegalArgumentException iae2) {
					return UNKNOWN;
				} catch (PEException e) {
					return UNKNOWN;
				}
			}

		}
		
		public static MysqlType toMysqlType(int nativeTypeId) {
			for (MysqlType v : values()) {
				if (v.sqlType == nativeTypeId)
					return v;
			}
			return null;
		}

		public boolean isValid() {
			return isValid;
		}

		public int getSqlType() {
			return sqlType;
		}

		public MyFieldType getMysqlFieldType() {
			return mft;
		}
		
		public NativeTypeAlias[] getAliases() {
			return aliases;
		}

		public boolean isJdbcType() {
			return jdbcType;
		}

		public static MysqlNativeTypeCatalog getMntCatalog() {
			return mntCatalog;
		}

		@Override
		public String toString() {
			return name;
		}

	}

//	@SuppressWarnings("unused")
//	private MysqlNativeType() {
//		// don't allow constructor without a type
//		this.mysqlType = null;
//	}

	/**
	 * Main constructor, based on the MysqlType enum
	 * 
	 * @param mysqlType
	 * @throws PEException
	 */
	public MysqlNativeType(MysqlType mysqlType) throws PEException {
		super(mysqlType.toString(), (int) mysqlType.getMysqlFieldType().getByteValue(), mysqlType.getSqlType(), mysqlType.isJdbcType(), mysqlType.getAliases());
		this.mysqlType = mysqlType;

		// all mysql types are nullable
		setNullability((short) DatabaseMetaData.typeNullable);
		setProperCaseSensitive();
		setProperUnsignedAttribute();

		if (mysqlType == MysqlType.UNKNOWN) {
			throw new PEException("Type name " + mysqlType + " is not a valid MySQL type");
		}
	}

	public MysqlType getMysqlType() {
		return mysqlType;
	}

	public byte getCharSet() {
		return charSet;
	}

	public void setCharSet(byte charSet) {
		this.charSet = charSet;
	}

	@SuppressWarnings("hiding")
	public MysqlNativeType withCharSet(byte charSet) {
		setCharSet(charSet);
		return this;
	}

	public short getFieldTypeFlags() {
		return fieldTypeFlags;
	}

	public void setFieldTypeFlags(short fieldTypeFlags) {
		this.fieldTypeFlags = fieldTypeFlags;
		// case sensitivity and unsigned are based on these flags, so (re)set them here
		setProperCaseSensitive();
		setProperUnsignedAttribute();
	}

	@SuppressWarnings("hiding")
	public MysqlNativeType withFieldTypeFlags(short fieldTypeFlags) {
		setFieldTypeFlags(fieldTypeFlags);
		return this;
	}

	@SuppressWarnings("hiding")
	public MysqlNativeType withFieldTypeFlags(Integer fieldTypeFlags) {
		setFieldTypeFlags(fieldTypeFlags.shortValue());
		return this;
	}

	/**
	 * Case sensitivity is hardcoded in mysql and cannot be set
	 *
	 * @return
	 */
	@Override
	public void setCaseSensitive(boolean caseSensitive) {
		throw new RuntimeException("Cannot set 'case sensitive' attribute in mysql (on type " + getTypeName() + ")");
	}

	/**
	 * In the MySQL DatabaseMetaData class there is a getTypeInfo() method that sets the case sensitive bit.
	 * However, that bit is not used by the official MySQL driver. It turns out case sensitive value is hardcoded,
	 * so we set it whenever a type is created
	 *
	 * @return
	 */
	private void setProperCaseSensitive() {
		boolean caseSensitive = false;

		int sqlType = getDataType();

		switch (sqlType) { // NOPMD by doug on 18/12/12 7:36 AM
		case Types.BIT:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.FLOAT:
		case Types.REAL:
		case Types.DOUBLE:
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:

			caseSensitive = false;
			break;

		case Types.NUMERIC:
		case Types.DECIMAL:
			caseSensitive = true;
			break;

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:

			if (isBinary()) {
				caseSensitive = true;
			}
			// TODO: Need to augment this to match official MySQL driver
			// Currently we know if the collation sequence is LATIN1_SWEDISH_CI
			// (default)
			// then we should return false.
			if (getCharSet() == MysqlNativeConstants.MYSQL_CHARSET_UTF8) {
				caseSensitive = false;
			}
			// String collationName = field.getCollation();
			//
			// return ((collationName != null) &&
			// !collationName.endsWith("_ci"));
			break;

		default:
			caseSensitive = true;
		}

		super.setCaseSensitive(caseSensitive);
	}

	/**
	 * Unsigned attribute is hardcoded in mysql and cannot be set
	 *
	 * @return
	 */
	@Override
	public void setUnsignedAttribute(boolean unsignedAttribute) {
		throw new RuntimeException("Cannot set 'unsigned' attribute in mysql");
	}

	/**
	 * In the MySQL DatabaseMetaData class there is a getTypeInfo() method that sets the unsigned attribute bit.
	 * However, that bit is not used by the official MySQL driver. It turns out unsigned attribute value is hardcoded,
	 * so we set it whenever a type is created
	 *
	 * @return
	 */
	private void setProperUnsignedAttribute() {
		boolean unsigned = false;
		int sqlType = getDataType();
	
		switch (sqlType) { // NOPMD by doug on 18/12/12 7:36 AM
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.DOUBLE:
		case Types.REAL:
		case Types.FLOAT:
		case Types.NUMERIC:
		case Types.DECIMAL:
			unsigned = !isUnsigned();
			break;

		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			unsigned = false;
			break;

		default:
			unsigned = false;
		}

		super.setUnsignedAttribute(unsigned);
	}

	@Override
	public boolean asKeyRequiresPrefix() {
		switch(mysqlType) {
		case MEDIUMBLOB:
		case LONGBLOB:
		case BLOB:
		case TINYBLOB:
		case MEDIUMTEXT:
		case LONGTEXT:
		case TEXT:
		case TINYTEXT:
			return true;
		default:
			return false;
		}
	}

	@Override
	public Object getZeroValue() throws PEException {
		switch(mysqlType) {
		case BIGINT:
		case BIT:
		case BOOL:
		case INT:
//		case INTEGER:
//		case NUMERIC:
		case SMALLINT:
		case MEDIUMINT:
		case TINYINT:
		case DOUBLE:
		case DECIMAL:
		case DOUBLE_PRECISION:
		case FLOAT:
			return new Integer(0);
		case CHAR:
//		case LONG_NVARCHAR:
//		case LONG_VARCHAR:
		case TEXT:
		case LONGTEXT:
		case MEDIUMTEXT:
//		case NCHAR:
//		case NVARCHAR:
		case TINYTEXT:
		case VARCHAR:
		case BLOB:
		case LONGBLOB:
		case ALTLONGBLOB:
//		case LONG_VARBINARY:
		case MEDIUMBLOB:
		case ALTMEDIUMBLOB:
		case TINYBLOB:
		case ALTTINYBLOB:
		case VARBINARY:
		case BINARY:
			return "";
		case DATE:
			return "0000-00-00";
		case TIME:
			return "00:00:00";
		case DATETIME:
		case TIMESTAMP:
			return "0000-00-00 00:00:00";
		case YEAR:
			return new Integer(0);
		default:
			throw new PEException("No zero value known for type " + mysqlType);
		}
	}
	
	@Override
	public boolean supportsDefaultValue() {
		switch(mysqlType) {
		case BLOB:
		case LONGBLOB:
		case MEDIUMBLOB:
		case TINYBLOB:
		case TEXT:
		case LONGTEXT:
		case MEDIUMTEXT:
		case TINYTEXT:
			return false;
		default:
			return true;
		}
	}

	@Override
	public boolean isTimestampType() {
		return mysqlType == MysqlType.TIMESTAMP;
	}

	
	private boolean isBinary() {
		return ((getFieldTypeFlags() & MysqlNativeConstants.FLDPKT_FLAG_BINARY) > 0);
	}

	private boolean isUnsigned() {
		return ((getFieldTypeFlags() & MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED) > 0);
	}

	@Override
	public String toString() {
		return "MysqlNativeType [mysqlType=" + mysqlType + ", charSet=" + charSet + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((mysqlType == null) ? 0 : mysqlType.hashCode());
		result = prime * result + charSet;
		result = prime * result + fieldTypeFlags;
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
		MysqlNativeType other = (MysqlNativeType) obj;
		if (mysqlType != other.mysqlType)
			return false;
		if (charSet != other.charSet)
			return false;
		if (fieldTypeFlags != other.fieldTypeFlags)
			return false;
		return true;
	}

}
