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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Types;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.db.mysql.libmy.TestColumnDef;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.standalone.PETest;

public class MysqlNativeTest {
	static Logger logger = Logger.getLogger(MysqlNativeTest.class);

	@BeforeClass
	public static void setup() throws Exception {
		TestCatalogHelper.createMinimalCatalog(PETest.class);
		TestHost.startServices(PETest.class);
	}

	@AfterClass
	public static void teardown() throws Exception {
		TestHost.stopServices();
	}

	@Test
	public void testNativeTypeCatalog() throws PEException {
        NativeType charType = Singletons.require(HostService.class).getDBNative().findType("CHAR"); // look up by top level name
        NativeType intType = Singletons.require(HostService.class).getDBNative().findType("INTEGER"); // look up by synonymn
        NativeType blobType = Singletons.require(HostService.class).getDBNative().findType("LONGBLOB"); // look up by synonymn

		assertTrue(charType.getDataType() == Types.CHAR);
		assertTrue(charType.requiresQuotes());
		assertTrue(charType.isComparable());

		assertTrue(intType.getDataType() == Types.INTEGER);
		assertFalse(intType.requiresQuotes());

		assertTrue(blobType.getDataType() == Types.LONGVARBINARY);
		assertFalse(blobType.isComparable());
	}

	@Test(expected = PEException.class)
	public void testFindUnknownType() throws PEException {
        Singletons.require(HostService.class).getDBNative().findType("UNKNOWN TYPE");
	}

	/*
	@Test
	public void testCreateTable() throws PEException, SQLException {
		DistributionModel dm = new BroadcastDistributionModel();
		PersistentGroup sg1 = new PersistentGroup("Default");
		UserDatabase udb = new UserDatabase(UserDatabase.DEFAULT, sg1);

		UserTable ut1 = new UserTable("native_syntax_test", dm, udb, TableState.SHARED, PEConstants.DEFAULT_DB_ENGINE, PEConstants.DEFAULT_TABLE_TYPE);

		// We will test 5 types as there are 5 cases of CREATE TABLE syntax related to types
		// - type has no precision or scale (e.g. DATE)
		// - type has precision (e.g. INT)
		// - type is signed (e.g. INT SIGNED)
		// - type has precision and scale (e.g. DECIMAL)
		// - type has size (e.g. CHAR)
		UserColumn uc = new UserColumn("col1", 1, "CHAR");
		uc.setSize(15);
		uc.setPrecision(15);
		uc.setScale(0);
		ut1.addUserColumn(uc);

		uc = new UserColumn("col2", 4, "INT");
		uc.setNativeTypeModifiers(MysqlNativeType.MODIFIER_UNSIGNED);
		uc.setSize(10);
		uc.setPrecision(10);
		uc.setScale(0);
		ut1.addUserColumn(uc);

		uc = new UserColumn("col3", 4, "INT");
		uc.setNativeTypeModifiers(MysqlNativeType.MODIFIER_SIGNED);
		uc.setSize(10);
		uc.setPrecision(10);
		uc.setScale(0);
		ut1.addUserColumn(uc);

		uc = new UserColumn("col4", 91, "DATE");
		uc.setSize(10);
		uc.setPrecision(0);
		uc.setScale(0);
		ut1.addUserColumn(uc);

		uc = new UserColumn("col5", 3, "DECIMAL");
		uc.setSize(7);
		uc.setPrecision(5);
		uc.setScale(4);
		ut1.addUserColumn(uc);

		logger.debug("Create table statement is: " + ut1.getCreateTableStmt());

		DBHelper dbh = new DBHelper(
				TestCatalogHelper.getTestCatalogProps(PETest.class))
				.connect();
		dbh.executeQuery(ut1.getCreateTableStmt());
		Statement stmt = dbh.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery("SELECT col1, col2, col3, col4, col5 FROM native_syntax_test where 0=1"); // NOPMD by doug on 30/11/12 3:13 PM
		ResultSetMetaData rsmd = rs.getMetaData();

        UserColumn ucRSMD1 = new UserColumn(Singletons.require(HostService.class).getDBNative().getResultSetColumnInfo(rsmd, null, 1));
        UserColumn ucRSMD2 = new UserColumn(Singletons.require(HostService.class).getDBNative().getResultSetColumnInfo(rsmd, null, 2));
        UserColumn ucRSMD3 = new UserColumn(Singletons.require(HostService.class).getDBNative().getResultSetColumnInfo(rsmd, null, 3));
        UserColumn ucRSMD4 = new UserColumn(Singletons.require(HostService.class).getDBNative().getResultSetColumnInfo(rsmd, null, 4));
        UserColumn ucRSMD5 = new UserColumn(Singletons.require(HostService.class).getDBNative().getResultSetColumnInfo(rsmd, null, 5));

		assertTrue(ut1.getUserColumn("col1").getDataType() == ucRSMD1.getDataType());
		assertTrue(ut1.getUserColumn("col2").getDataType() == ucRSMD2.getDataType());
		assertTrue(ut1.getUserColumn("col3").getDataType() == ucRSMD3.getDataType());
		assertTrue(ut1.getUserColumn("col4").getDataType() == ucRSMD4.getDataType());
		assertTrue(ut1.getUserColumn("col5").getDataType() == ucRSMD5.getDataType());

		assertTrue(ut1.getUserColumn("col1").getNativeTypeName().equals(ucRSMD1.getNativeTypeName()));
		assertTrue(ut1.getUserColumn("col2").getNativeTypeName().equals(ucRSMD2.getNativeTypeName()));
		assertTrue(ut1.getUserColumn("col3").getNativeTypeName().equals(ucRSMD3.getNativeTypeName()));
		assertTrue(ut1.getUserColumn("col4").getNativeTypeName().equals(ucRSMD4.getNativeTypeName()));
		assertTrue(ut1.getUserColumn("col5").getNativeTypeName().equals(ucRSMD5.getNativeTypeName()));

		assertTrue(ucRSMD1.getNativeTypeModifiers() == null);
		assertTrue(ut1.getUserColumn("col2").getNativeTypeModifiers().equals(ucRSMD2.getNativeTypeModifiers()));
		assertTrue(ucRSMD3.getNativeTypeModifiers() == null);
		assertTrue(ucRSMD4.getNativeTypeModifiers() == null);
		assertTrue(ucRSMD5.getNativeTypeModifiers() == null);

		assertTrue(ut1.getUserColumn("col1").getPrecision() == ucRSMD1.getPrecision());
		assertTrue(ut1.getUserColumn("col2").getPrecision() == ucRSMD2.getPrecision());
		assertTrue(ut1.getUserColumn("col3").getPrecision() == ucRSMD3.getPrecision());
		assertTrue(ut1.getUserColumn("col4").getPrecision() == ucRSMD4.getPrecision());
		assertTrue(ut1.getUserColumn("col5").getPrecision() == ucRSMD5.getPrecision());

		assertTrue(ut1.getUserColumn("col1").getScale() == ucRSMD1.getScale());
		assertTrue(ut1.getUserColumn("col2").getScale() == ucRSMD2.getScale());
		assertTrue(ut1.getUserColumn("col3").getScale() == ucRSMD3.getScale());
		assertTrue(ut1.getUserColumn("col4").getScale() == ucRSMD4.getScale());
		assertTrue(ut1.getUserColumn("col5").getScale() == ucRSMD5.getScale());

		stmt.close();
		dbh.disconnect();
	}
	*/

	@Test
	public void testTypeLookup() throws PEException {
		class TestData {
			TestColumnDef tcd = null;
			MysqlType mt = null;

			TestData(TestColumnDef tcd, MysqlType mt)
			{
				this.tcd = tcd;
				this.mt = mt;
			}
		}
		TestData[] testData = {
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 65535),
						MysqlType.TEXT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 255),
						MysqlType.TINYTEXT),
				new TestData(
						new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777215),
						MysqlType.MEDIUMTEXT),
				new TestData(
						new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777216),
						MysqlType.LONGTEXT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BIT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 1),
						MysqlType.BIT),
				// new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DECIMAL, 0, 0), MysqlType.DECIMAL),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 4),
						MysqlType.TINYINT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 3), MysqlType.TINYINT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 6),
						MysqlType.SMALLINT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 5), MysqlType.SMALLINT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 11),
						MysqlType.INT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 12),
						MysqlType.FLOAT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DOUBLE, MysqlNativeConstants.FLDPKT_FLAG_NUM, 22),
						MysqlType.DOUBLE),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NULL, 0, 0), MysqlType.NULL),

				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TIMESTAMP,
						MysqlNativeConstants.FLDPKT_FLAG_NOT_NULL
								+ MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
								+ MysqlNativeConstants.FLDPKT_FLAG_ZEROFILL
								+ MysqlNativeConstants.FLDPKT_FLAG_BINARY
								+ MysqlNativeConstants.FLDPKT_FLAG_ON_UPDATE_NOW, 19), MysqlType.TIMESTAMP),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 20),
						MysqlType.BIGINT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 9),
						MysqlType.MEDIUMINT),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DATE, 0, 0), MysqlType.DATE),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TIME, 0, 0), MysqlType.TIME),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DATETIME, 0, 0), MysqlType.DATETIME),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_YEAR,
						MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
								+ MysqlNativeConstants.FLDPKT_FLAG_ZEROFILL
								+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 4), MysqlType.YEAR),
				// new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDATE, 0, 0), MysqlType.DATE),
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAGS_BINARY_FLAG,
//						39), MysqlType.NUMERIC),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_ENUM, 0, 0), MysqlType.ENUM),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SET, 0, 0), MysqlType.SET),
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY_BLOB,
//						MysqlNativeConstants.FLDPKT_FLAGS_BINARY_FLAG
//								+ MysqlNativeConstants.FLDPKT_FLAGS_BLOB_FLAG, 255), MysqlType.TINYBLOB),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB,
						MysqlNativeConstants.FLDPKT_FLAG_BINARY
								+ MysqlNativeConstants.FLDPKT_FLAG_BLOB, 255), MysqlType.TINYBLOB),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY_BLOB,
						MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255), MysqlType.ALTTINYBLOB),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG_BLOB,
						MysqlNativeConstants.FLDPKT_FLAG_BINARY + MysqlNativeConstants.FLDPKT_FLAG_NOT_NULL, 16777216), MysqlType.ALTLONGBLOB),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB,
						MysqlNativeConstants.FLDPKT_FLAG_BINARY
								+ MysqlNativeConstants.FLDPKT_FLAG_BLOB, 65535), MysqlType.BLOB),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB,
						MysqlNativeConstants.FLDPKT_FLAG_BINARY
								+ MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777215), MysqlType.MEDIUMBLOB),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_MEDIUM_BLOB,
						MysqlNativeConstants.FLDPKT_FLAG_BINARY, 16777215), MysqlType.ALTMEDIUMBLOB),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB,
						MysqlNativeConstants.FLDPKT_FLAG_BINARY
								+ MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777216), MysqlType.LONGBLOB),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, 0, 255), MysqlType.VARCHAR),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, 0, 10), MysqlType.CHAR),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 10),
						MysqlType.BINARY),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING,
						MysqlNativeConstants.FLDPKT_FLAG_NOT_NULL, 2048), MysqlType.VARCHAR),
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING,
								MysqlNativeConstants.FLDPKT_FLAG_BINARY, 2048), MysqlType.VARBINARY),
				// new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_GEOMETRY, 0, 0), MysqlType.GEOMETRY),
								
				// Data generated by 'all_types' table - there may be dupes here due to aliases, transforms, etc.
				// Column: bigint_aka_bigint_20
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 20),
						MysqlType.BIGINT),
				// Column: bigint_unsigned_aka_bigint_20_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 20), MysqlType.BIGINT),
				// Column: bigint_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 1),
						MysqlType.BIGINT),
				// Column: bigint_1_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 1), MysqlType.BIGINT),
				// Column: bigint_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 10),
						MysqlType.BIGINT),
				// Column: bigint_255
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 255),
						MysqlType.BIGINT),
				// Column: bigint_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 64),
						MysqlType.BIGINT),
				// Column: bigint_65
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 65),
						MysqlType.BIGINT),
				// Column: binary_aka_binary_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 1),
						MysqlType.BINARY),
				// Column: binary_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 1),
						MysqlType.BINARY),
				// Column: binary_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 10),
						MysqlType.BINARY),
				// Column: binary_255
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255),
						MysqlType.BINARY),
				// Column: binary_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 64),
						MysqlType.BINARY),
				// Column: binary_65
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65),
						MysqlType.BINARY),
				// Column: bit_aka_bit_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BIT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED, 1),
						MysqlType.BIT),
				// Column: bit_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BIT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED, 1),
						MysqlType.BIT),
				// Column: bit_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BIT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED, 10),
						MysqlType.BIT),
				// Column: bit_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BIT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED, 64),
						MysqlType.BIT),
				// Column: blob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65535), MysqlType.BLOB),
				// Column: blob_1_aka_tinyblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255), MysqlType.TINYBLOB),
				// Column: blob_10_aka_tinyblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255), MysqlType.TINYBLOB),
				// Column: blob_16777215_aka_mediumblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 16777215), MysqlType.MEDIUMBLOB),
				// Column: blob_16777216_aka_longblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 16777216), MysqlType.LONGBLOB),
				// Column: blob_4294967295L_aka_longblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 4294967295L), MysqlType.LONGBLOB),
				// Column: blob_21844_aka_blob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65535), MysqlType.BLOB),
				// Column: blob_21845_aka_blob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65535), MysqlType.BLOB),
				// Column: blob_21846_aka_blob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65535), MysqlType.BLOB),
				// Column: blob_255_aka_tinyblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255), MysqlType.TINYBLOB),
				// Column: blob_256_aka_blob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65535), MysqlType.BLOB),
				// Column: blob_32768_aka_blob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65535), MysqlType.BLOB),
				// Column: blob_64_aka_tinyblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255), MysqlType.TINYBLOB),
				// Column: blob_65_aka_tinyblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255), MysqlType.TINYBLOB),
				// Column: blob_65535_aka_blob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65535), MysqlType.BLOB),
				// Column: blob_65536_aka_mediumblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 16777215), MysqlType.MEDIUMBLOB),

				// These should be TINYINTs !!		
				// Column: bool_aka_tinyint
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 1),
						MysqlType.BOOL),
				// Column: boolean_aka_tinyint
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 1),
						MysqlType.BOOL),
						
				// Column: char_aka_char_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 3),
						MysqlType.CHAR),
				// Column: char_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 3),
						MysqlType.CHAR),
				// Column: char_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 30),
						MysqlType.CHAR),
				// Column: char_255
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 765),
						MysqlType.CHAR),
				// Column: char_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 192),
						MysqlType.CHAR),
				// Column: char_65
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 195),
						MysqlType.CHAR),
				// Column: character_aka_char
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 3),
						MysqlType.CHAR),
				// Column: date
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DATE, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 10),
						MysqlType.DATE),
				// Column: datetime
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DATETIME, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 19),
						MysqlType.DATETIME),
				// Column: dec_aka_decimal
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_NUM, 11),
						MysqlType.DECIMAL),
				// Column: decimal_aka_decimal_10_0
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_NUM, 11),
						MysqlType.DECIMAL),
				// Column: decimal_unsigned_aka_decimal_10_0_uns
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 10), MysqlType.DECIMAL),
				// Column: decimal_1_aka_decimal_1_0
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_NUM, 2),
						MysqlType.DECIMAL),
				// Column: decimal_1_unsigned_aka_decimal_1_0_un
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 1), MysqlType.DECIMAL),
				// Column: decimal_10_aka_decimal_10_0
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_NUM, 11),
						MysqlType.DECIMAL),
				// Column: decimal_2_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_NUM, 4),
						MysqlType.DECIMAL),
				// Column: decimal_2_1_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 3), MysqlType.DECIMAL),
				// Column: decimal_64_aka_decimal_64_0
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_NUM, 65),
						MysqlType.DECIMAL),
				// Column: decimal_65_aka_decimal_65_0
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_NUM, 66),
						MysqlType.DECIMAL),
				// Column: double
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DOUBLE, MysqlNativeConstants.FLDPKT_FLAG_NUM, 22),
						MysqlType.DOUBLE),
				// Column: double_unsigned_aka_double_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DOUBLE, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 22), MysqlType.DOUBLE),
				// Column: double_2_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DOUBLE, MysqlNativeConstants.FLDPKT_FLAG_NUM, 2),
						MysqlType.DOUBLE),
				// Column: double_2_1_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DOUBLE, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 2), MysqlType.DOUBLE),
				// Column: double_precision__aka_double
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DOUBLE, MysqlNativeConstants.FLDPKT_FLAG_NUM, 22),
						MysqlType.DOUBLE),
				// Column: float
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 12),
						MysqlType.FLOAT),
				// Column: float_unsigned_aka_float_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 12), MysqlType.FLOAT),
				// Column: float_1_aka_float
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 12),
						MysqlType.FLOAT),
				// Column: float_1_unsigned_aka_float_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 12), MysqlType.FLOAT),
				// Column: float_10_aka_float
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 12),
						MysqlType.FLOAT),
				// Column: float_2_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 2),
						MysqlType.FLOAT),
				// Column: float_2_1_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 2), MysqlType.FLOAT),
				// Column: float4_aka_float
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_FLOAT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 12),
						MysqlType.FLOAT),
				// Column: float8_aka_double
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DOUBLE, MysqlNativeConstants.FLDPKT_FLAG_NUM, 22),
						MysqlType.DOUBLE),
				// Column: int_aka_int_11
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 11),
						MysqlType.INT),
				// Column: int_unsigned_aka_int_10_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 10), MysqlType.INT),
				// Column: int_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 1),
						MysqlType.INT),
				// Column: int_1_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 1), MysqlType.INT),
				// Column: int_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 10),
						MysqlType.INT),
				// Column: int_255
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 255),
						MysqlType.INT),
				// Column: int_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 64),
						MysqlType.INT),
				// Column: int_65
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 65),
						MysqlType.INT),
				// Column: int1_aka_tinyint
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 4),
						MysqlType.TINYINT),
				// Column: int2_aka_smallint
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 6),
						MysqlType.SMALLINT),
				// Column: int3_aka_mediumint
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 9),
						MysqlType.MEDIUMINT),
				// Column: int4_aka_int
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 11),
						MysqlType.INT),
				// Column: int8_aka_bigint
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONGLONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 20),
						MysqlType.BIGINT),
				// Column: integer_aka_int
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_LONG, MysqlNativeConstants.FLDPKT_FLAG_NUM, 11),
						MysqlType.INT),
				// Column: long_aka_mediumtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777215),
						MysqlType.MEDIUMTEXT),
				// Column: long_varbinary__aka_mediumblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 16777215), MysqlType.MEDIUMBLOB),

//              // This is a MEDIUMTEXT with UTF_8, so the length is 3 times as long - we don't support that yet, think it's
//				// a LONGTEXT
//				// Column: long_varchar__aka_mediumtext
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 50331645),
//						MysqlType.MEDIUMTEXT),
				
				// Column: longblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 4294967295L), MysqlType.LONGBLOB),
				// Column: longtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 4294967295L),
						MysqlType.LONGTEXT),
				// Column: mediumblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 16777215), MysqlType.MEDIUMBLOB),
				// Column: mediumint_aka_mediumint_9
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 9),
						MysqlType.MEDIUMINT),
				// Column: mediumint_unsigned_aka_mediumint_8_un
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 8), MysqlType.MEDIUMINT),
				// Column: mediumint_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 1),
						MysqlType.MEDIUMINT),
				// Column: mediumint_1_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 1), MysqlType.MEDIUMINT),
				// Column: mediumint_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 10),
						MysqlType.MEDIUMINT),
				// Column: mediumint_255
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 255),
						MysqlType.MEDIUMINT),
				// Column: mediumint_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 64),
						MysqlType.MEDIUMINT),
				// Column: mediumint_65
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 65),
						MysqlType.MEDIUMINT),
				// Column: mediumtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777215),
						MysqlType.MEDIUMTEXT),
				// Column: middleint_aka_mediumint
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_INT24, MysqlNativeConstants.FLDPKT_FLAG_NUM, 9),
						MysqlType.MEDIUMINT),
				// Column: nchar_aka_char_1_CHARACTER_SET_utf8
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 3),
						MysqlType.CHAR),
				// Column: nchar_1_aka_char_1_CHARACTER_SET_utf
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 3),
						MysqlType.CHAR),
				// Column: numeric_aka_decimal
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_NEWDECIMAL, MysqlNativeConstants.FLDPKT_FLAG_NUM, 11),
						MysqlType.DECIMAL),
				// Column: nvarchar_1_aka_varchar_1_CHARACTER_S
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 3),
						MysqlType.VARCHAR),
				// Column: nvarchar_10_aka_varchar_10_CHARACTER
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 30),
						MysqlType.VARCHAR),
				// Column: nvarchar_16777215_aka_longtext_CHARA
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 4294967295L),
						MysqlType.LONGTEXT),
				// Column: nvarchar_16777216_aka_longtext_CHARA
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 4294967295L),
						MysqlType.LONGTEXT),
						
//	            // This is a MEDIUMTEXT with UTF_8, so the length is 3 times as long - we don't support that yet, think it's
//				// a LONGTEXT
//				// Column: nvarchar_21846_aka_mediumtext_CHARAC
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 50331645),
//						MysqlType.MEDIUMTEXT),

				// Column: nvarchar_255_aka_varchar_255_CHARACT
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 765),
						MysqlType.VARCHAR),
				// Column: nvarchar_256_aka_varchar_256_CHARACT
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 768),
						MysqlType.VARCHAR),
						
//              // This is a MEDIUMTEXT with UTF_8, so the length is 3 times as long - we don't support that yet, think it's
//				// a LONGTEXT
//				// Column: nvarchar_32768_aka_mediumtext_CHARAC
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 50331645),
//						MysqlType.MEDIUMTEXT),
				// Column: nvarchar_64_aka_varchar_64_CHARACTER
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 192),
						MysqlType.VARCHAR),
				// Column: nvarchar_65_aka_varchar_65_CHARACTER
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 195),
						MysqlType.VARCHAR),
				// Column: real_aka_double
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_DOUBLE, MysqlNativeConstants.FLDPKT_FLAG_NUM, 22),
						MysqlType.DOUBLE),
				// Column: smallint_aka_smallint_6
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 6),
						MysqlType.SMALLINT),
				// Column: smallint_unsigned_aka_smallint_5_uns
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 5), MysqlType.SMALLINT),
				// Column: smallint_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 1),
						MysqlType.SMALLINT),
				// Column: smallint_1_unsigned
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 1), MysqlType.SMALLINT),
				// Column: smallint_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 10),
						MysqlType.SMALLINT),
				// Column: smallint_255
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 255),
						MysqlType.SMALLINT),
				// Column: smallint_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 64),
						MysqlType.SMALLINT),
				// Column: smallint_65
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_SHORT, MysqlNativeConstants.FLDPKT_FLAG_NUM, 65),
						MysqlType.SMALLINT),
				// Column: text
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 65535),
						MysqlType.TEXT),
	
//	            // This is a TEXT with UTF_8, so the length is 3 times as long - we don't support that yet, think it's
//				// a MEDIUMTEXT
//				// Column: text
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 196605),
//						MysqlType.TEXT),
						
				// Column: text_1_aka_tinytext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 255),
						MysqlType.TINYTEXT),

//			    // This is a TINYTEXT with UTF_8, so the length is 3 times as long - we don't support that yet, think it's
//				// a TEXT
//				// Column: text_10_aka_tinytext
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 765),
//						MysqlType.TINYTEXT),
								
				// Column: text_16777215_aka_mediumtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777215),
						MysqlType.MEDIUMTEXT),
				// Column: text_16777216_aka_longtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777216),
						MysqlType.LONGTEXT),
				// Column: text_21844_aka_text
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 21844),
						MysqlType.TEXT),
				// Column: text_21845_aka_text
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 21845),
						MysqlType.TEXT),
				// Column: text_21846_aka_text
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 21846),
						MysqlType.TEXT),
				// Column: text_255_aka_tinytext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 255),
						MysqlType.TINYTEXT),
				// Column: text_256_aka_text
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 256),
						MysqlType.TEXT),
				// Column: text_32768_aka_text
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 32768),
						MysqlType.TEXT),
				// Column: text_64_aka_tinytext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 64),
						MysqlType.TINYTEXT),
				// Column: text_65_aka_tinytext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 65),
						MysqlType.TINYTEXT),
				// Column: text_65536_aka_mediumtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 65536),
						MysqlType.MEDIUMTEXT),
				// Column: time
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TIME, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 8),
						MysqlType.TIME),
				// Column: timestamp_aka_timestamp_NOT_NULL_DEF
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TIMESTAMP, MysqlNativeConstants.FLDPKT_FLAG_NOT_NULL
						+ MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED + MysqlNativeConstants.FLDPKT_FLAG_ZEROFILL
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY + MysqlNativeConstants.FLDPKT_FLAG_TIMESTAMP
						+ MysqlNativeConstants.FLDPKT_FLAG_ON_UPDATE_NOW, 19), MysqlType.TIMESTAMP),
				// Column: tinyblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255), MysqlType.TINYBLOB),
				// Column: tinyint_aka_tinyint_4
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 4),
						MysqlType.TINYINT),
				// Column: tinyint_unsigned_aka_tinyint_3_unsig
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 3), MysqlType.TINYINT),
						
//				// These come out as BOOL since we haven't fixed that yet (BOOLs are TINYINT(1) - shouldn't be actual type)
//				// Column: tinyint_1
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 1),
//						MysqlType.TINYINT),
//				// Column: tinyint_1_unsigned
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
//						+ MysqlNativeConstants.FLDPKT_FLAG_NUM, 1), MysqlType.TINYINT),
						
				// Column: tinyint_4
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 4),
						MysqlType.TINYINT),

//				// These don't work, because BOOL is around, once it's gone they will
//				// Column: tinyint_10
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 10),
//						MysqlType.TINYINT),
//				// Column: tinyint_255
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 255),
//						MysqlType.TINYINT),
//				// Column: tinyint_64
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 64),
//						MysqlType.TINYINT),
//				// Column: tinyint_65
//				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_TINY, MysqlNativeConstants.FLDPKT_FLAG_NUM, 65),
//						MysqlType.TINYINT),
						
				// Column: tinytext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 255),
						MysqlType.TINYTEXT),
				// Column: varbinary_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 1),
						MysqlType.VARBINARY),
				// Column: varbinary_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 10),
						MysqlType.VARBINARY),
				// Column: varbinary_16777215_aka_mediumblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 16777215), MysqlType.MEDIUMBLOB),
				// Column: varbinary_16777216_aka_longblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 4294967295L), MysqlType.LONGBLOB),
				// Column: varbinary_255
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 255),
						MysqlType.VARBINARY),
				// Column: varbinary_256
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 256),
						MysqlType.VARBINARY),
				// Column: varbinary_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 64),
						MysqlType.VARBINARY),
				// Column: varbinary_65
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_BINARY, 65),
						MysqlType.VARBINARY),
				// Column: varbinary_65536_aka_mediumblob
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB
						+ MysqlNativeConstants.FLDPKT_FLAG_BINARY, 16777215), MysqlType.MEDIUMBLOB),
				// Column: varchar_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 3),
						MysqlType.VARCHAR),
				// Column: varchar_10
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 30),
						MysqlType.VARCHAR),
				// Column: varchar_16777215_aka_mediumtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 16777215),
						MysqlType.MEDIUMTEXT),
				// Column: varchar_16777216_aka_longtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 4294967295L),
						MysqlType.LONGTEXT),
				// Column: varchar_255
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 255),
						MysqlType.VARCHAR),
				// Column: varchar_256
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 256),
						MysqlType.VARCHAR),
				// Column: varchar_64
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 64),
						MysqlType.VARCHAR),
				// Column: varchar_65
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 65),
						MysqlType.VARCHAR),
				// Column: varchar_65536_aka_mediumtext
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_BLOB, MysqlNativeConstants.FLDPKT_FLAG_BLOB, 65536),
						MysqlType.MEDIUMTEXT),
				// Column: varcharacter_1_aka_varchar_1
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NONE, 1),
						MysqlType.VARCHAR),
				// Column: varcharacter_0
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_VAR_STRING, MysqlNativeConstants.FLDPKT_FLAG_NOT_NULL, 0),
						MysqlType.VARCHAR),
				// Column: year_aka_year_4
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_YEAR, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_ZEROFILL + MysqlNativeConstants.FLDPKT_FLAG_NUM, 4), MysqlType.YEAR),
				// Column: year_1_aka_year_4
				new TestData(new TestColumnDef(MyFieldType.FIELD_TYPE_YEAR, MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED
						+ MysqlNativeConstants.FLDPKT_FLAG_ZEROFILL + MysqlNativeConstants.FLDPKT_FLAG_NUM, 4), MysqlType.YEAR),
		};

        MysqlNative msn = (MysqlNative) Singletons.require(HostService.class).getDBNative();
		int i = 0;
		MysqlNativeType mnt;
		for (TestData rec : testData) {
			MyFieldType mft = rec.tcd.getFieldType();
			mnt = msn.getNativeTypeFromMyFieldType(mft, rec.tcd.getFlags(), rec.tcd.getMaxLen(1));
			assertNotNull(mnt);
			assertEquals("Invalid map of field type - loop index is: " + i,
					rec.mt.name(), NativeType.fixNameForType(mnt.getTypeName()));
			i++;
		}
	}

}
