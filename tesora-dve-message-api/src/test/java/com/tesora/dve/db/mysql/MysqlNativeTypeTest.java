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
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEBaseTest;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.exceptions.PEException;

public class MysqlNativeTypeTest extends PEBaseTest {
	
	/**
	 * Define the type to zero value translation table. NULL represents undefined values.
	 * @see testGetZeroValue()
	 */
	private static final Map<MysqlType, Object> TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE = new HashMap<MysqlType, Object>();
	static {
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.BIGINT, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.BIT, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.BOOL, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.INT, new Integer(0));
//		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.INTEGER, new Integer(0));
//		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.NUMERIC, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.SMALLINT, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.MEDIUMINT, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.TINYINT, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.DOUBLE, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.DECIMAL, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.DOUBLE_PRECISION, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.FLOAT, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.CHAR, new String(""));
//		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.LONG_NVARCHAR, new String(""));
//		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.LONG_VARCHAR, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.TEXT, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.LONGTEXT, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.MEDIUMTEXT, new String(""));
//		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.NCHAR, new String(""));
//		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.NVARCHAR, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.TINYTEXT, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.VARCHAR, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.BLOB, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.LONGBLOB, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.ALTLONGBLOB, new String(""));
//		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.LONG_VARBINARY, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.MEDIUMBLOB, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.ALTMEDIUMBLOB, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.TINYBLOB, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.ALTTINYBLOB, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.VARBINARY, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.BINARY, new String(""));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.DATE, new String("0000-00-00"));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.TIME, new String("00:00:00"));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.DATETIME, new String("0000-00-00 00:00:00"));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.TIMESTAMP, new String("0000-00-00 00:00:00"));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.YEAR, new Integer(0));
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.NULL, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.PARAMETER, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.ENUM, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.SET, null);
//		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.REAL_UNUSED, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.GEOMETRY, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.POINT, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.LINESTRING, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.POLYGON, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.GEOMETRYCOLLECTION, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.MULTIPOINT, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.MULTILINESTRING, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.MULTIPOLYGON, null);
		TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.put(MysqlType.UNKNOWN, null);
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		applicationName = "MysqlNativeTypeTest";
		logger = Logger.getLogger(MysqlNativeTypeTest.class);
	}

	@Test
	public void testMysqlNativeType() {
		try {
			for (MysqlType mysqlType : MysqlType.values()) {
				if (mysqlType.isValid()) {
					MysqlNativeType mnType = new MysqlNativeType(mysqlType);
					for (String name : mnType.getNames()) {
						assertTrue("Type " + name + " should be lower case",
								name.equals(name.toLowerCase(Locale.ENGLISH)));
						MysqlNativeType actualType = new MysqlNativeType(MysqlType.toMysqlType(name));
						String msg = "Should find correct type for alias: " + name;
						if (mnType.getTypeName().equals(name)) {
							msg = "Should find correct type for actual type: " + name;
						}
						assertEquals(msg, mnType, actualType);
					}
				}
			}
		} catch (Exception e) {
			failWithStackTrace(e);
		}
	} 
	
	@Test
	/** Reproduce PE-798 */
	public void testGetZeroValue() {
		try {
			for (final MysqlType mysqlType : MysqlType.values()) {
				if (mysqlType.isValid()) {
					// Check that we test all available types.
					assertTrue("Missing mysqlType " + mysqlType,
							TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.containsKey(mysqlType));
					final MysqlNativeType mnType = new MysqlNativeType(mysqlType);
					final Object expectedValue = TYPE_TO_ZERO_VALUE_TRANSLATION_TABLE.get(mysqlType);
					if (expectedValue != null) {
						assertEquals(expectedValue, mnType.getZeroValue());
					} else {
						/* This type does not have a valid zero value. */
						new ExpectedExceptionTester() {
							@Override
							public void test() throws PEException {
								mnType.getZeroValue();
							}
						}.assertException(PEException.class);
					}
				}
			}
		} catch (final Exception e) {
			failWithStackTrace(e);
		}
	}

	@SuppressWarnings("unused")
	@Test(expected = PEException.class)
	public void testUnknownMysqlNativeType() throws PEException {
		new MysqlNativeType(MysqlType.UNKNOWN);
	}


}
