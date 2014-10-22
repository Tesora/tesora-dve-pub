package com.tesora.dve.db.mysql.common;

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

import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.resultset.ColumnSet;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.time.FastDateFormat;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;

public class DBTypeBasedUtils {
	
//	private static final Logger logger = Logger.getLogger(DBTypeBasedUtils.class);
	
    public static Map<Integer, DataTypeValueFunc> SQLTypeValueFuncMap =
    		new HashMap<Integer, DataTypeValueFunc>();
    public static Map<MyFieldType, DataTypeValueFunc> MysqlTypeValueFuncMap =
    		new HashMap<MyFieldType, DataTypeValueFunc>();
    public static Map<Class<?>, DataTypeValueFunc> JavaTypeValueFuncMap = 
    		new HashMap<Class<?>, DataTypeValueFunc>();
    
    static {
    	SQLTypeValueFuncMap.put(Types.BIT, new BitValueFunc());
    	SQLTypeValueFuncMap.put(Types.TINYINT, new ByteValueFunc());
    	SQLTypeValueFuncMap.put(Types.SMALLINT, new ShortValueFunc());
    	SQLTypeValueFuncMap.put(Types.INTEGER, new IntegerValueFunc());
    	SQLTypeValueFuncMap.put(Types.BIGINT, new LongValueFunc());
    	SQLTypeValueFuncMap.put(Types.FLOAT, new FloatValueFunc());
    	SQLTypeValueFuncMap.put(Types.DOUBLE, new DoubleValueFunc());
    	SQLTypeValueFuncMap.put(Types.REAL, new FloatValueFunc());
    	SQLTypeValueFuncMap.put(Types.NUMERIC, new DecimalValueFunc());
    	SQLTypeValueFuncMap.put(Types.DECIMAL, new DecimalValueFunc());
    	SQLTypeValueFuncMap.put(Types.CHAR, new CharValueFunc());
    	SQLTypeValueFuncMap.put(Types.VARCHAR, new CharValueFunc());
    	SQLTypeValueFuncMap.put(Types.LONGVARCHAR, new CharValueFunc());
    	SQLTypeValueFuncMap.put(Types.DATE, new DateValueFunc());
    	SQLTypeValueFuncMap.put(Types.TIME, new TimeValueFunc());
    	SQLTypeValueFuncMap.put(Types.TIMESTAMP, new TimestampValueFunc());
    }

    static {
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_BIT, new BitValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_TINY, new TinyValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_DECIMAL, new DecimalValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_DOUBLE, new DoubleValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_FLOAT, new FloatValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_INT24, new MediumValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_LONG, new IntegerValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_LONGLONG, new LongValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_NEWDECIMAL, new DecimalValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_DECIMAL, new DecimalValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_SHORT, new ShortValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_NULL, new NullValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_VARCHAR, new CharValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_VAR_STRING, new CharValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_STRING, new CharValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_BLOB, new CharValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_LONG_BLOB, new CharValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_MEDIUM_BLOB, new CharValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_TINY_BLOB, new CharValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_NEWDATE, new DateValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_DATE, new DateValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_TIME, new TimeValueFunc());	
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_DATETIME, new DateTimeValueFunc());
    	MysqlTypeValueFuncMap.put(MyFieldType.FIELD_TYPE_TIMESTAMP, new TimestampValueFunc());
    }
    
    static {
    	for ( DataTypeValueFunc dtvf : SQLTypeValueFuncMap.values() ) {
    		if ( dtvf.getJavaClass() != null ) {
        		JavaTypeValueFuncMap.put( dtvf.getJavaClass(), dtvf);
    		}
    	}
    }
    
    static DataTypeValueFunc BINARY_VALUE_FUNC = new BinaryValueFunc();
    static DataTypeValueFunc BYTE_VALUE_FUNC = new ByteValueFunc();
    

    public static DataTypeValueFunc getJavaTypeFunc(Class<?> clazz) {
    	if (!JavaTypeValueFuncMap.containsKey(clazz))
    		throw new IllegalArgumentException("Java Type " + clazz + " not found in type map");

    	return JavaTypeValueFuncMap.get(clazz);
    }
    
    public static DataTypeValueFunc getMysqlTypeFunc(MyFieldType type) throws PEException {
    	return getMysqlTypeFunc(type, 0, 0);
    }
    
    public static DataTypeValueFunc getMysqlTypeFunc(MyFieldType type, int length, int flags) throws PEException {
    	// Special case for binary() and tinyint - unfortunately...
    	if (MyFieldType.FIELD_TYPE_STRING.equals(type) && (flags & MysqlNativeConstants.FLDPKT_FLAG_BINARY) > 0)
    		return BINARY_VALUE_FUNC;
    	else if (MyFieldType.FIELD_TYPE_TINY.equals(type) && length == 1)
    		return BYTE_VALUE_FUNC;
    	
    	if (!MysqlTypeValueFuncMap.containsKey(type))
    		throw new PEException("Mysql type " + type + " not found in type map");
    	
    	return MysqlTypeValueFuncMap.get(type);
    }

    public static DataTypeValueFunc getMysqlTypeFunc(ColumnMetadata columnMetadata) throws PEException {
        return getMysqlTypeFunc(MyFieldType.fromByte(columnMetadata.getNativeTypeId()), columnMetadata.getSize(), columnMetadata.getNativeTypeFlags());
    }

    public static List<DataTypeValueFunc> getMysqlTypeFunctions(ColumnSet columns) throws PEException {
        List<DataTypeValueFunc> funcs = new ArrayList<>();
        for (ColumnMetadata meta : columns.getColumnList())
            funcs.add(getMysqlTypeFunc(meta));
        return funcs;
    }

    public static DataTypeValueFunc getSQLTypeFunc(int type) throws PEException {
    	if (!SQLTypeValueFuncMap.containsKey(type))
    		throw new PEException("SQL type " + type + " not found in type map");

    	return SQLTypeValueFuncMap.get(type);
    }

	/**
	 * The BIT data type is used to store bit-field values. A type of BIT(M)
	 * enables storage of M-bit values. M can range from 1 to 64.
	 * MySQL Binary Protocol handles BIT data as length encoded strings.
	 * 
	 * @see http://dev.mysql.com/doc/refman/5.6/en/bit-type.html
	 * @see https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
	 */
	static class BitValueFunc implements DataTypeValueFunc {
		@Override
		public void writeObject(ByteBuf cb, Object value) {
			if (value instanceof Boolean) {
				MysqlAPIUtils.putLengthCodedString(cb, BooleanUtils.toIntegerObject((Boolean) value));
			} else {
				MysqlAPIUtils.putLengthCodedString(cb, value);
			}
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return MysqlAPIUtils.getLengthCodedBinary(cb);
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return new String((byte[]) value, CharsetUtil.ISO_8859_1);
		}

		@Override
		public String getMysqlTypeName() {
			return "BIT";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return value.getBytes(CharsetUtil.ISO_8859_1);
		}

		@Override
		public Class<?> getJavaClass() {
			return byte[].class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_BIT;
		}
    }

	static class ByteValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			byte byteValue=0;
			if ( value instanceof Byte )
				byteValue = (Byte) value;
			else if ( value instanceof Integer )
				byteValue = ((Integer) value).byteValue();
			else 
				throw new IllegalArgumentException("Type " + value.getClass().getSimpleName() + " not handled by ByteValueFunc");

			cb.writeByte(byteValue);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return cb.readByte();
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return value.toString();
		}

		@Override
		public String getMysqlTypeName() {
			return "BYTE";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return Byte.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return Byte.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_TINY;
		}
    }

	static class TinyValueFunc extends ByteValueFunc {
		@Override
		public Object readObject(ByteBuf cb) {
			return Integer.valueOf(cb.readByte());
		}

		@Override
		public Class<?> getJavaClass() {
			return null;
		}
	}

	static class ShortValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			short shortValue=0;
			if ( value instanceof Short )
				shortValue = (Short) value;
			else if ( value instanceof Integer )
				shortValue = ((Integer) value).shortValue();
			else 
				throw new IllegalArgumentException("Type " + value.getClass().getSimpleName() + " not handled by ShortValueFunc");

			cb.writeShort(shortValue);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return cb.readShort();
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return ((Short) value).toString();
		}

		@Override
		public String getMysqlTypeName() {
			return "SHORT";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			// This is so that we match the behaviour of the mysql jdbc driver (so Proxy-based tests
			// will pass.
			if ( colMd.getSize() >= 5 )
				return Integer.valueOf(value);
			
			return Short.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return Short.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_SHORT;
		}
    }

	static class CharValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			MysqlAPIUtils.putLengthCodedString(cb, (String) value, false);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return MysqlAPIUtils.getLengthCodedString(cb);
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			// escape any embedded single quotes that are not already escaped
			String in = (String) value;
			String out = null;
			if (pstmt) {
				out = escape(in);
			} else {
				StringBuilder buf = new StringBuilder(in.length() + 2);
				buf.append("'").append(in).append("'");
				out = buf.toString();
			}
			return out;
		}
		

		@Override
		public String getMysqlTypeName() {
			return "CHAR";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return value;
		}

		@Override
		public Class<?> getJavaClass() {
			return String.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_VAR_STRING;
		}
    }

	public static String escape(String in) {
		int len = in.length();
		StringBuilder out = new StringBuilder((int) (len * 1.1));
		out.append("'");
		if (len == 0) {
			out.append("'");
			return out.toString();
		}
			
		char p = in.charAt(0);
		if (p == '\'' || p == '\\') {
			out.append('\\');
		}
		out.append(p);
		for(int i = 1; i < len; i++) {
			char c = in.charAt(i);
			if ((c == '\'' || c == '\\') /*&& p != '\\'*/)
				out.append('\\');
			
			out.append(c);
			p = c;
		}
		out.append("'");
		return out.toString();			
	}

	
	static class BinaryValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			MysqlAPIUtils.putLengthCodedString(cb, value);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return MysqlAPIUtils.getLengthCodedBinary(cb);
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return "'" + value + "'";
		}

		@Override
		public String getMysqlTypeName() {
			return "BINARY";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return value.getBytes();
		}

		@Override
		public Class<?> getJavaClass() {
			return byte[].class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_BLOB;
		}
    }

	static class IntegerValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			if ( value instanceof Long )
				cb.writeInt(((Long) value).intValue());
			else
				cb.writeInt((Integer) value);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return cb.readInt();
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			if (value instanceof Long) {
				return ((Long) value).toString();
			}
			return ((Integer) value).toString();
		}

		@Override
		public String getMysqlTypeName() {
			return "INT";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			if ( colMd.isUnsigned() )
				return Long.valueOf(value);
			
			return Integer.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return Integer.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_LONG;
		}
	}

	static class MediumValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			cb.writeInt((Integer) value);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return cb.readInt();
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return ((Integer) value).toString();
		}

		@Override
		public String getMysqlTypeName() {
			return "INT24";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return Integer.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return null;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_INT24;
		}
    }

	static class LongValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			if ( value instanceof BigInteger )
				cb.writeLong(((BigInteger) value).longValue());
			else
				cb.writeLong((Long) value);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return cb.readLong();
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			if (value instanceof BigInteger) {
				return ((BigInteger) value).toString();
			}
			return ((Long) value).toString();
		}

		@Override
		public String getMysqlTypeName() {
			return "LONGLONG";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return Long.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return Long.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_LONGLONG;
		}
    }
	
	static class FloatValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			cb.writeFloat((Float) value);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return cb.readFloat();
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return ((Float) value).toString();
		}

		@Override
		public String getMysqlTypeName() {
			return "FLOAT";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return Float.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return Float.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_FLOAT;
		}
    }

	static class DoubleValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			cb.writeDouble((Double) value);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return cb.readDouble();
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return ((Double) value).toString();
		}

		@Override
		public String getMysqlTypeName() {
			return "DOUBLE";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return Double.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return Double.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_DOUBLE;
		}
    }

	static class DecimalValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			MysqlAPIUtils.putLengthCodedString(cb, ((BigDecimal) value).toPlainString(), false);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return new BigDecimal(MysqlAPIUtils.getLengthCodedString(cb));
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return ((BigDecimal) value).toPlainString();
		}

		@Override
		public String getMysqlTypeName() {
			return "DECIMAL";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return new BigDecimal(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return BigDecimal.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_DECIMAL;
		}
    }

	static class NullValueFunc implements DataTypeValueFunc {
		@Override
        public void writeObject(ByteBuf cb, Object value) {
			cb.writeZero(1);
		}

		@Override
		public Object readObject(ByteBuf cb) {
			return null;
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			return "null";
		}

		@Override
		public String getMysqlTypeName() {
			return "NULL";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return null;
		}

		@Override
		public Class<?> getJavaClass() {
			return null;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_NULL;
		}
    }
	
	static class DateValueFunc implements DataTypeValueFunc {

		@Override
        public void writeObject(ByteBuf cb, Object value) {
			MysqlAPIUtils.putLengthCodedDate(cb, (Date) value);
		}

		@Override
		public Object readObject(ByteBuf cb) throws PEException {
			return MysqlAPIUtils.getLengthCodedDate(cb);
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			FastDateFormat fdf = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_DATE_FORMAT);
			
			return "'" + fdf.format((Date) value) + "'";
		}

		@Override
		public String getMysqlTypeName() {
			return "DATE";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			Date dateValue = null;
			// Alas, the FastDateFormat doesn't support parse...
			try {
				 dateValue = new SimpleDateFormat(MysqlNativeConstants.MYSQL_DATE_FORMAT, Locale.ENGLISH).parse(value);
			} catch (ParseException e) {
				throw new PEException("Cannot convert value to Date type", e);
			}
			return dateValue;
		}

		@Override
		public Class<?> getJavaClass() {
			return Date.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_DATE;
		}
	}

	static class DateTimeValueFunc extends DateValueFunc {

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			FastDateFormat fdf = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_DATETIME_FORMAT);
			
			return "'" + fdf.format((Date) value) + "'";
		}

		@Override
		public String getMysqlTypeName() {
			return "DATETIME";
		}

		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return Timestamp.valueOf(value);
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_DATETIME;
		}

	}

	static class TimestampValueFunc extends DateValueFunc {

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			FastDateFormat fdf = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_TIMESTAMP_FORMAT);
			
			return "'" + fdf.format((Date) value) + "'";
		}

		@Override
		public String getMysqlTypeName() {
			return "TIMESTAMP";
		}

		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return Timestamp.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return Timestamp.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_TIMESTAMP;
		}

	}
	
	static class TimeValueFunc implements DataTypeValueFunc {

		@Override
        public void writeObject(ByteBuf cb, Object value) {
			MysqlAPIUtils.putLengthCodedTime(cb, (Time) value);
		}

		@Override
		public Object readObject(ByteBuf cb) throws PEException {
			return MysqlAPIUtils.getLengthCodedTime(cb);
		}

		@Override
		public String getParamReplacement(Object value, boolean pstmt) {
			FastDateFormat fdf = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_TIME_FORMAT_MS);
			
			return "'" + fdf.format((Time) value) + "'";
		}

		@Override
		public String getMysqlTypeName() {
			return "TIME";
		}
		
		@Override
		public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
			return Time.valueOf(value);
		}

		@Override
		public Class<?> getJavaClass() {
			return Time.class;
		}

		@Override
		public MyFieldType getMyFieldType() {
			return MyFieldType.FIELD_TYPE_TIME;
		}
	}
}
