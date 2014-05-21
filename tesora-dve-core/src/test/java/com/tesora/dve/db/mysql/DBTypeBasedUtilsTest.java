// OS_STATUS: public
package com.tesora.dve.db.mysql;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.tesora.dve.db.mysql.common.DataTypeValueFunc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.sql.Time;
import java.sql.Types;
import java.text.ParseException;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.junit.Test;

import com.tesora.dve.common.PEBaseTest;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class DBTypeBasedUtilsTest extends PEBaseTest {

	private static byte[] BIT_DATA = new byte[] { (byte) 1, (byte) 1, (byte) 0, (byte) 1, (byte) 1, (byte) 0, (byte) 1 };

	static ListOfPairs<MyFieldType,Object> expValuesMysql = new ListOfPairs<MyFieldType,Object>();
	static {
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_BIT, BIT_DATA));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_TINY, new Byte((byte) 0xFD)));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_SHORT, new Short((short) 5678)));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_LONGLONG, new Long(8765432L)));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_LONG, new Integer(8765432)));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_INT24, new Integer(876432)));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_DECIMAL, new BigDecimal("12345.6789")));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_DOUBLE, new Double(12345.6789)));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_FLOAT, new Float(12345.6789)));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_VARCHAR, new String("hi there")));
		expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_NULL, null));
		try {
			expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_DATE, DateUtils.parseDate("2012-10-11", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS)));
			expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_DATETIME, DateUtils.parseDate("2012-10-11 12:34:33", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS)));
			expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_TIME, new Time(DateUtils.parseDate("12:34:33", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS).getTime())));
			expValuesMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_TIMESTAMP, DateUtils.parseDate("2012-10-11 12:34:33.543", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS)));
		} catch (ParseException e) {
			fail(e.getMessage());
		}
	}
	
	static ListOfPairs<Integer,Object> expValuesSQL = new ListOfPairs<Integer,Object>();
	static {
		expValuesSQL.add(new Pair<Integer, Object>(Types.BIT, BIT_DATA));
		expValuesSQL.add(new Pair<Integer,Object>(Types.CHAR, new String("hi there")));
		expValuesSQL.add(new Pair<Integer,Object>(Types.INTEGER, new Integer(123456)));
		expValuesSQL.add(new Pair<Integer,Object>(Types.BIGINT, new Long(234233234)));
		expValuesSQL.add(new Pair<Integer,Object>(Types.FLOAT, new Float(23423.234)));
		expValuesSQL.add(new Pair<Integer,Object>(Types.DOUBLE, new Double(2423423.23423)));
		expValuesSQL.add(new Pair<Integer,Object>(Types.DECIMAL, new BigDecimal("23424.23442")));
		try {
			expValuesSQL.add(new Pair<Integer,Object>(Types.TIME, new Time(DateUtils.parseDate("12:34:56", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS).getTime())));
			expValuesSQL.add(new Pair<Integer,Object>(Types.DATE, DateUtils.parseDate("2012-01-31", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS)));
		} catch (ParseException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void mysqlReadWriteTest() throws Exception {
		ByteBuf cb = Unpooled.buffer(100).order(ByteOrder.LITTLE_ENDIAN);

		int len;
		for (Pair<MyFieldType, Object> expValue : expValuesMysql) {
			cb.clear();
			len = 0;
			if ( expValue.getSecond() instanceof Byte )
				len = 1;
			DataTypeValueFunc dtvf = DBTypeBasedUtils.getMysqlTypeFunc(expValue.getFirst(), len, 0);
			dtvf.writeObject(cb, expValue.getSecond());
			assertEqualData(expValue.getSecond(), dtvf.readObject(cb));
		}
	}
	
	@Test
	public void mysqlConvertToObjectTest() throws Exception {
		ColumnMetadata colMd = new ColumnMetadata();
		FastDateFormat fdfDate = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_DATE_FORMAT);
		FastDateFormat fdfDateTime = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_DATETIME_FORMAT);
		FastDateFormat fdfTime = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_TIME_FORMAT);
		FastDateFormat fdfTimestamp = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_TIMESTAMP_FORMAT);
		
		for (Pair<MyFieldType, Object> expValue : expValuesMysql) {
			DataTypeValueFunc dtvf = DBTypeBasedUtils.getMysqlTypeFunc(expValue.getFirst());
			assertNotNull("Couldn't find function for " + expValue.getFirst(), dtvf);
			if ( expValue.getSecond() != null ) {
				String value;
				if ( MyFieldType.FIELD_TYPE_DATE.equals(expValue.getFirst()) ) {
					value = fdfDate.format(expValue.getSecond());
				} else if ( MyFieldType.FIELD_TYPE_DATETIME.equals(expValue.getFirst()) ) {
						value = fdfDateTime.format(expValue.getSecond());
				} else if ( MyFieldType.FIELD_TYPE_TIME.equals(expValue.getFirst()) ) {
					value = fdfTime.format(expValue.getSecond());
				} else if ( MyFieldType.FIELD_TYPE_TIMESTAMP.equals(expValue.getFirst()) ) {
					value = fdfTimestamp.format(expValue.getSecond());
				} else if (MyFieldType.FIELD_TYPE_BIT.equals(expValue.getFirst())) {
					value = new String((byte[]) expValue.getSecond(), CharsetUtil.ISO_8859_1);
				} else {
					value = expValue.getSecond().toString();
				}
				
				Object valueObj = dtvf.convertStringToObject(value, colMd);
				assertEqualData(expValue.getSecond(), valueObj);
			}
		}		
	}
	
	@Test (expected=PEException.class)
	public void mysqlFailTest() throws Exception {
		DBTypeBasedUtils.getMysqlTypeFunc(MyFieldType.FIELD_TYPE_ENUM);
	}

	@Test
	public void mysqlConvertTest() throws Exception {
		ByteBuf cb;

		ListOfPairs<MyFieldType,Object> expValuesConvMysql = new ListOfPairs<MyFieldType,Object>();
		expValuesConvMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_LONG, new Integer(8765432)));
		expValuesConvMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_LONGLONG, new Long(8765432)));
		expValuesConvMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_SHORT, new Short((short) 5678)));
		expValuesConvMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_TINY, new Integer(100)));
		expValuesConvMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_TINY, new Byte((byte) 1)));
//		expValuesConvMysql.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_BIT, new Boolean(true)));

		int len;
		for (Pair<MyFieldType, Object> expValue : expValuesConvMysql) {
			len = 0;
			if ( expValue.getSecond() instanceof Byte )
				len = 1;

			cb = Unpooled.buffer(100).order(ByteOrder.LITTLE_ENDIAN);
			DataTypeValueFunc dtvf = DBTypeBasedUtils.getMysqlTypeFunc(expValue.getFirst(), len, 0);
			dtvf.writeObject(cb, expValue.getSecond());
			assertEquals(expValue.getSecond(), dtvf.readObject(cb) );
		}
		
	}
	
	@Test
	public void sqlReadWriteTest() throws Exception {
		ByteBuf cb = Unpooled.buffer(100).order(ByteOrder.LITTLE_ENDIAN);

		for (Pair<Integer, Object> expValue : expValuesSQL) {
			cb.clear();
			DataTypeValueFunc dtvf = DBTypeBasedUtils.getSQLTypeFunc(expValue.getFirst());
			dtvf.writeObject(cb, expValue.getSecond());
			assertEqualData(expValue.getSecond(), dtvf.readObject(cb));
		}
	}

	@Test (expected=PEException.class)
	public void sqlFailTest() throws Exception {
		DBTypeBasedUtils.getSQLTypeFunc(Types.ARRAY);
	}
	
	@Test
	public void javaTypesTest() throws Exception {
		for (Pair<MyFieldType, Object> expValue : expValuesMysql) {
			if ( expValue.getSecond() != null ) {
				DataTypeValueFunc dtvf = DBTypeBasedUtils.getJavaTypeFunc(expValue.getSecond().getClass());
				assertEquals(expValue.getSecond().getClass(), dtvf.getJavaClass());
			}
		}
	}
}
