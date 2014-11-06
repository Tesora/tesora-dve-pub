package com.tesora.dve.db;

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

import io.netty.util.CharsetUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import com.tesora.dve.common.PELongUtils;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.sql.ConversionException;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.types.Type;

public class ValueConverter {

	public static final ValueConverter INSTANCE = new ValueConverter();
	
	private ValueConverter() {
	}

	private abstract static class Converter<T> {

		protected Class<?> targetClass;
		protected int[] sqlTypes;
		protected int[] literalTypes;

		private Converter(Class<?> targ, int[] matchesSqlTypes, int[] matchesLiteralTypes) {
			targetClass = targ;
			sqlTypes = matchesSqlTypes;
			literalTypes = matchesLiteralTypes;
		}

		private boolean handles(int[] set, int value) {
			if (set == null)
				return false;
			for (int i = 0; i < set.length; i++)
				if (set[i] == value)
					return true;
			return false;
		}

		public boolean handlesSqlType(int typeCode) {
			return handles(sqlTypes, typeCode);
		}

		public boolean handlesLiteralType(int literalTypeCode) {
			return handles(literalTypes, literalTypeCode);
		}

		protected abstract T convertString(String in, int literalKind) throws Exception;

		// subtypes can choose to implement; otherwise throw an exception
		protected T convertObject(Object in, Type toType) {
			throw unsupportedConversion(in, toType);
		}

		public T convert(String in, int literalKind) {
			try {
				return convertString(in, literalKind);
			} catch (final NumberFormatException e) {
				throw e; // Possibly handled by using a different number converter.
			} catch (final Exception e) {
				throw conversionException(in, e);
			}
		}

		@SuppressWarnings("unchecked")
		public T convert(Object in, Type toType) {
			if (in == null)
				return null;
			if (in instanceof String) {
				try {
					return convertString((String) in, 0);
				} catch (Exception e) {
					throw conversionException(in, e);
				}
			} else if (targetClass.isInstance(in))
				return (T) targetClass.cast(in);
			else
				return convertObject(in, toType);
		}

		protected ConversionException unsupportedConversion(Object inputValue, Type toType) throws ConversionException {
			return new ConversionException(Pass.PLANNER, "Conversion from '" + inputValue + "' to "
					+ targetClass.getName() + "(" + toType.getName() + ") is not supported yet.");
		}

		protected ConversionException conversionException(Object inputValue, Exception e) throws ConversionException {
			return new ConversionException(Pass.SECOND, "Not a " + targetClass.getName() + ": '" + inputValue + "'", e);
		}

	}

	// default converters
	private static class StringConverter extends Converter<String> {

		public StringConverter() {
			super(String.class, new int[] { Types.CHAR, Types.NCHAR, Types.NVARCHAR, Types.VARCHAR }, new int[] {
					TokenTypes.Character_String_Literal, TokenTypes.National_Character_String_Literal,
					TokenTypes.Bit_String_Literal, TokenTypes.Hex_String_Literal, TokenTypes.NULL });
		}

		@Override
		protected String convertString(String in, int literalKind) {
			if (in.length() == 0) return in;
			if ((in.charAt(0) == '\'' && in.endsWith("'")) || (in.charAt(0) == '\"' && in.endsWith("\""))) {
				// strip off the quotes
				return in.substring(1, in.length() - 1);
			}
			return in;
		}

		@Override
		protected String convertObject(Object in, Type toType) {
			return in.toString();
		}

	}

	private static class IntegerConverter extends Converter<Long> {

		public IntegerConverter() {
			super(Long.class, new int[] { Types.INTEGER },
					new int[] { TokenTypes.Signed_Integer, TokenTypes.Unsigned_Integer });
		}

		@Override
		protected Long convertString(String in, int literalKind) throws Exception {
			String t = in.trim();
			if (t.startsWith("+")) //NOPMD
				t = t.substring(1);
			return PELongUtils.decode(t);
		}

	}

	private static class ByteConverter extends Converter<Byte> {

		public ByteConverter() {
			super(Byte.class, new int[] { Types.TINYINT }, null);
		}

		@Override
		protected Byte convertString(String in, int literalKind) throws Exception {
			return Byte.valueOf(in);
		}

		@Override
		public Byte convert(Object in, Type toType) {
			if (in == null)
				return null;
			if (in instanceof Long)
				// literals usually come in as longs
				return Byte.valueOf(((Long) in).byteValue());
			return super.convert(in, toType);
		}

	}

	private static class ShortConverter extends Converter<Short> {

		public ShortConverter() {
			super(Short.class, new int[] { Types.SMALLINT }, null);
		}

		@Override
		protected Short convertString(String in, int literalKind) throws Exception {
			return Short.valueOf(in);
		}

		@Override
		public Short convert(Object in, Type toType) {
			if (in == null)
				return null;
			if (in instanceof Long)
				// literals usually come in as longs
				return Short.valueOf(((Long) in).shortValue());
			return super.convert(in, toType);
		}

	}

	private static class LongConverter extends Converter<Long> {
		public LongConverter() {
			super(Long.class, null, new int[] { TokenTypes.Unsigned_Integer, TokenTypes.Signed_Large_Integer });
		}

		@Override
		protected Long convertString(String in, int literalKind) throws Exception {
			return Long.valueOf(in);
		}

	}

	private static class BigIntegerConverter extends Converter<BigInteger> {
		public BigIntegerConverter() {
			super(BigInteger.class, new int[] { Types.BIGINT }, new int[] { TokenTypes.Unsigned_Integer, TokenTypes.Unsigned_Large_Integer });
		}

		@Override
		protected BigInteger convertString(String in, int literalKind) throws Exception {
			return new BigInteger(in);
		}

		@Override
		public BigInteger convert(Object in, Type toType) {
			if (in == null)
				return null;
			if (in instanceof Long)
				// literals usually come in as longs
				return BigInteger.valueOf(((Long) in).longValue());
			return super.convert(in, toType);
		}
	}

	private static class DoubleConverter extends Converter<Double> {

		public DoubleConverter() {
			super(Double.class, new int[] { Types.DOUBLE }, null);
		}

		@Override
		protected Double convertString(String in, int literalKind) throws Exception {
			return Double.valueOf(in);
		}

	}

	private static class FloatConverter extends Converter<Double> {

		public FloatConverter() {
			super(Float.class, new int[] { Types.DOUBLE }, new int[] { TokenTypes.Unsigned_Float, TokenTypes.Signed_Float });
		}

		@Override
		protected Double convertString(String in, int literalKind) throws Exception {
			// up convert to double - or should we just go all the way to big
			// decimal?
			return Double.valueOf(in);
		}
	}

	private static class BinaryConverter extends Converter<String> {

		public BinaryConverter() {
			super(String.class, new int[] { Types.BINARY }, null);
		}

		@Override
		protected String convertString(String in, int literalKind) throws Exception {
			if (in.length() == 0) return in;
			if (in.charAt(0) == '\'' && in.endsWith("'")) {
				// strip off the quotes
				return in.substring(1, in.length() - 1);
			}
			return in;
		}

	}

	private static class DecimalConverter extends Converter<BigDecimal> {

		public DecimalConverter() {
			super(BigDecimal.class, new int[] { Types.DECIMAL }, null);
		}

		@Override
		protected BigDecimal convertString(String in, int literalKind) throws Exception {
			return BigDecimal.valueOf(Double.valueOf(in));
		}

	}

	private static class DateConverter extends Converter<Date> {

		public DateConverter() {
			super(Date.class, new int[] { Types.DATE, Types.TIMESTAMP }, null);
		}

		@Override
		protected Date convertString(String in, int literalKind) throws Exception {
			if (StringUtils.isBlank(in)) 
				return null;
			return DateUtils.parseDate(in, MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS);
		}

	}

	private static class TimeConverter extends Converter<Time> {

		public TimeConverter() {
			super(Time.class, new int[] { Types.TIME }, null);
		}

		@Override
		protected Time convertString(String in, int literalKind) throws Exception {
			if (StringUtils.isBlank(in)) 
				return null;
			return new Time(DateUtils.parseDate(in, MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS).getTime());
		}

	}

	private static class BooleanConverter extends Converter<Boolean> {
		
		public BooleanConverter() {
			super(Boolean.TYPE, new int[] { Types.BOOLEAN }, new int[] { TokenTypes.TRUE, TokenTypes.FALSE } );
		}

		@Override
		protected Boolean convertString(String in, int literalKind)
				throws Exception {
			return Boolean.valueOf(in);
		}
	}
	
	private static Converter<?>[] defaultConverters = new Converter[] { new StringConverter(),
			new ShortConverter(), new IntegerConverter(), new LongConverter(), new BigIntegerConverter(),
			new FloatConverter(), new DoubleConverter(), new DecimalConverter(),
			new BinaryConverter(), new ByteConverter(),
			new DateConverter(), new TimeConverter(),
			new BooleanConverter() };

	public Object convertLiteral(String in, int kind) {
		Converter<?>[] converters = defaultConverters;
		for (int i = 0; i < converters.length; i++) {
			if (converters[i].handlesLiteralType(kind)) {
				try {
					return converters[i].convert(in, kind);
				} catch (final NumberFormatException e) {
					// Try the next converter that can handle this type.
				}
			}
		}

		throw new ParserException(Pass.SECOND, "Unknown literal kind: " + TokenTypes.tokenNames[kind]);
	}

	// for inserts, we need to build the dv, and for that, we need actual values
	// after parsing, many literals are just strings, and need further
	// manipulation
	// q: should we do enforcement?
	public Object convert(ConnectionValues cv, ConstantExpression in, Type toType) {
		Object inValue = in.getValue(cv);
		return convert(inValue, toType);
	}

	public Object convert(Object inValue, Type toType) {
		Converter<?>[] converters = defaultConverters;
		for (int i = 0; i < converters.length; i++) {
			if (converters[i].handlesSqlType(toType.getBaseType().getDataType()))
				return converters[i].convert(inValue, toType);
		}
		throw new ConversionException(Pass.PLANNER, "Could not find converter for sqltype: " + toType.getName());
	}

	// also for inserts, we need to take the persisted default value and turn it
	// back into an object
	public Object convert(String persValue, Type toType) {
		Converter<?>[] converters = defaultConverters;
		for (int i = 0; i < converters.length; i++) {
			if (converters[i].handlesSqlType(toType.getBaseType().getDataType()))
				return converters[i].convert(persValue, toType);
		}
		throw new ConversionException(Pass.PLANNER, "Fill me in: convert to sqltype: " + toType.getName());
	}

	// handy for converting string literals
	public String convertStringLiteral(String in) {
		return (String) convertLiteral(in, TokenTypes.Character_String_Literal);
	}

	public byte[] convertBinaryLiteral(Object in) {
		if (in instanceof String) {
			return ((String)in).getBytes(CharsetUtil.ISO_8859_1);
		}
		throw new ConversionException(Pass.PLANNER, "Unknown object type for convertBinaryLiteral: "
				+ in.getClass().getName());
	}

	public static List<Byte> toObject(byte[] in) {
		ArrayList<Byte> out = new ArrayList<Byte>();
		for (int i = 0; i < in.length; i++)
			out.add(new Byte(in[i]));
		return out;
	}

	public static byte[] toPrimitive(List<Byte> in) {
		byte[] out = new byte[in.size()];
		for (int i = 0; i < out.length; i++) {
			out[i] = in.get(i).byteValue();
		}
		return out;
	}

}
