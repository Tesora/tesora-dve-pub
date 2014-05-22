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

import java.sql.Types;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEBaseTest;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.db.mysql.MysqlNativeTypeCatalog;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;

public class MysqlNativeTypeCatalogTest extends PEBaseTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		applicationName = "MysqlNativeTypeTest";
		logger = Logger.getLogger(MysqlNativeTypeTest.class);
	}

	@Test
	public void testLoad() {
		try {
			MysqlNativeTypeCatalog mntCatalog = new MysqlNativeTypeCatalog();
			mntCatalog.load();
			for (MysqlType mysqlType : MysqlType.values()) {
				if (mysqlType.isValid()) {
					MysqlNativeType mysqlNativeType = (MysqlNativeType) mntCatalog.findType(mysqlType.toString(), true);
					MysqlNativeType mnType = new MysqlNativeType(mysqlType);
					assertTrue("Type " + mysqlNativeType + " should be the same as the catalog type",
							mysqlNativeType.getMysqlType().equals(mnType.getMysqlType()));
				}
			}

			//mntCatalog.getTypesByName();
		} catch (Exception e) {
			failWithStackTrace(e);
		}
	}

	@Test
	public void testFindBySqlDataType() {
		try {
			// format sql type, mysqltype
			// Types commented out are not supported (at present?)
			String[][] data = {
					{ String.valueOf(Types.BIT), "BIT" },
					{ String.valueOf(Types.TINYINT), "TINYINT" },
					{ String.valueOf(Types.SMALLINT), "SMALLINT" },
					{ String.valueOf(Types.INTEGER), "INTEGER" },
					{ String.valueOf(Types.BIGINT), "BIGINT" },
					{ String.valueOf(Types.REAL), "FLOAT" },
//					{ String.valueOf(Types.FLOAT), "REAL_UNUSED" },
					{ String.valueOf(Types.DOUBLE), "DOUBLE" },
//					{ String.valueOf(Types.NUMERIC), "NUMERIC" },
					{ String.valueOf(Types.DECIMAL), "DECIMAL" },
					{ String.valueOf(Types.CHAR), "CHAR" },
					{ String.valueOf(Types.VARCHAR), "VARCHAR" },
//					{ String.valueOf(Types.LONGVARCHAR), "LONG_VARCHAR" },
					{ String.valueOf(Types.DATE), "DATE" },
					{ String.valueOf(Types.TIME), "TIME" },
					{ String.valueOf(Types.TIMESTAMP), "DATETIME" },
					{ String.valueOf(Types.BINARY), "BINARY" },
					{ String.valueOf(Types.VARBINARY), "VARBINARY" },
//					{ String.valueOf(Types.LONGVARBINARY), "LONG_VARBINARY" },
					{ String.valueOf(Types.NULL), "NULL" },
					//{ String.valueOf(Types.OTHER), "OTHER" },
					//{ String.valueOf(Types.JAVA_OBJECT), "JAVA_OBJECT" },
					//{ String.valueOf(Types.DISTINCT), "DISTINCT" },
					//{ String.valueOf(Types.STRUCT), "STRUCT" },
					//{ String.valueOf(Types.ARRAY), "ARRAY" },
					//{ String.valueOf(Types.BLOB), "BLOB" },
					//{ String.valueOf(Types.CLOB), "CLOB" },
					//{ String.valueOf(Types.REF), "REF" },
					//{ String.valueOf(Types.DATALINK), "DATALINK" },
					//{ String.valueOf(Types.BOOLEAN), "BOOLEAN" },
					//{ String.valueOf(Types.ROWID), "ROWID" },
//					{ String.valueOf(Types.NCHAR), "NCHAR" },
//					{ String.valueOf(Types.NVARCHAR), "NVARCHsAR" },
					//{ String.valueOf(Types.LONGNVARCHAR), "LONG_NVARCHAR" },
					//{ String.valueOf(Types.NCLOB), "NCLOB" },
					//{ String.valueOf(Types.SQLXML), "SQLXML" },
			};

			MysqlNativeTypeCatalog mntCatalog = new MysqlNativeTypeCatalog();
			mntCatalog.load();

			for (String[] rec : data) {
				int sqlType = Integer.valueOf(rec[0]);
				String mysqlTypeStr = rec[1];

				MysqlNativeType mysqlNativeType = (MysqlNativeType) mntCatalog.findType(sqlType, true);
				MysqlNativeType expectedMnType = (MysqlNativeType) mntCatalog.findType(mysqlTypeStr, true);
				assertEquals("Sql type " + sqlType + " should map to correct mysqlType",
						expectedMnType, mysqlNativeType);
			}

			//mntCatalog.getTypesByName();
		} catch (Exception e) {
			failWithStackTrace(e);
		}
	}

}
