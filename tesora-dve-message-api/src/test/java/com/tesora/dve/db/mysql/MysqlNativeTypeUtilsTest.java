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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.tesora.dve.common.PEBaseTest;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.db.mysql.MysqlNativeTypeUtils;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.resultset.ColumnMetadata;

public class MysqlNativeTypeUtilsTest extends PEBaseTest {

	@Test
	public void testIsUnsignedColumnMetadataMysqlNativeType() {
		try {
			assertTrue("Bit native type should return true",
					MysqlNativeTypeUtils.isUnsigned(null, new MysqlNativeType(MysqlType.BIT)));
			assertFalse("Signed varchar should return false",
					MysqlNativeTypeUtils.isUnsigned(null, new MysqlNativeType(MysqlType.VARCHAR)));
			assertFalse("Signed date should return false",
					MysqlNativeTypeUtils.isUnsigned(null, new MysqlNativeType(MysqlType.DATE)));

			MysqlType[] mysqlTypes = {
					MysqlType.TINYINT,
					MysqlType.SMALLINT,
					MysqlType.INT,
//					MysqlType.INTEGER,
					MysqlType.BIGINT,
					MysqlType.FLOAT,
					MysqlType.DOUBLE,
//					MysqlType.NUMERIC,
					MysqlType.DECIMAL,
			};
			for (MysqlType mysqlType : mysqlTypes) {
				ColumnMetadata cm = new ColumnMetadata();
				assertFalse(mysqlType.toString() + " should return false",
						MysqlNativeTypeUtils.isUnsigned(cm, new MysqlNativeType(MysqlType.INT)));
				cm.setUnsigned(true);
				assertTrue(mysqlType.toString() + " should return true",
						MysqlNativeTypeUtils.isUnsigned(cm, new MysqlNativeType(MysqlType.INT)));
			}
		} catch (Exception e) {
			failWithStackTrace(e);
		}
	}

	@Test
	public void testIsUnsignedColumnMetadata() {
		assertFalse("Null cm should return false", MysqlNativeTypeUtils.isUnsigned(null));
		ColumnMetadata cm = new ColumnMetadata();
		assertFalse("Empty cm should return false", MysqlNativeTypeUtils.isUnsigned(cm));
//		cm.setTypeSuffix("bogus");
//		assertFalse("Unrelated modifier should return false", MysqlNativeTypeUtils.isUnsigned(cm));
		cm.setUnsigned(true);
		assertTrue("Unsigned modifier should return true", MysqlNativeTypeUtils.isUnsigned(cm));
	}

}
