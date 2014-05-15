// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import io.netty.util.CharsetUtil;

import java.math.BigDecimal;
import java.sql.Time;
import java.text.ParseException;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;

import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class MyParameterTest {

	private static byte[] BIT_DATA = new byte[] { (byte) 1, (byte) 1, (byte) 0, (byte) 1, (byte) 1, (byte) 0, (byte) 1 };

	static ListOfPairs<MyFieldType,Object> expValues = new ListOfPairs<MyFieldType,Object>();
	static {
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_BIT, BIT_DATA));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_TINY, new Byte((byte) 0xFD)));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_SHORT, new Short((short) 5678)));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_LONGLONG, new Long(8765432L)));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_LONG, new Integer(8765432)));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_INT24, new Integer(876432)));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_DECIMAL, new BigDecimal("12345.6789")));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_DOUBLE, new Double(12345.6789)));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_FLOAT, new Float(12345.6789)));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_VARCHAR, new String("hi there")));
		expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_NULL, null));
		try {
			expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_DATE, DateUtils.parseDate("2012-10-11", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS)));
			expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_DATETIME, DateUtils.parseDate("2012-10-11 12:34:33", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS)));
			expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_TIME, new Time(DateUtils.parseDate("12:34:33", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS).getTime())));
			expValues.add(new Pair<MyFieldType, Object>(MyFieldType.FIELD_TYPE_TIMESTAMP, DateUtils.parseDate("2012-10-11 12:34:33.543", MysqlNativeConstants.MYSQL_DATE_FORMAT_PATTERNS)));
		} catch (ParseException e) {
			fail(e.getMessage());
		}
	};
	static String[] expStrings = {
			new String(BIT_DATA, CharsetUtil.ISO_8859_1),
		"-3",
		"5678",
		"8765432",
		"8765432",
		"876432",
		"12345.6789",
		"12345.6789",
		"12345.679",
		"'hi there'",
		"null",
		"'2012-10-11'",
		"'2012-10-11 12:34:33'",
		"'12:34:33.0'",
		"'2012-10-11 12:34:33.543'",
	};

	@Test
	public void allTypesTest() throws Exception {
		int i = 0;
		for (Pair<MyFieldType, Object> oneValue : expValues) {
			MyParameter param = new MyParameter(oneValue.getFirst(), oneValue.getSecond());
			assertEquals("on index " + i + " type " + param.getType(), expStrings[i], param.getValueForQuery());
			i++;
		}
	}
	
	@Test (expected=PEException.class) 
	public void failureTest() throws PEException {
		MyParameter param = new MyParameter(MyFieldType.FIELD_TYPE_ENUM, null);
		param.getValueForQuery();
	}
}
