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

import static com.tesora.dve.db.mysql.MysqlNativeConstants.MYSQL_CHARSET_BINARY;
import static com.tesora.dve.db.mysql.MysqlNativeConstants.MYSQL_CHARSET_UTF8;

import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.exceptions.PEException;

public class MysqlNativeTypeCatalog extends NativeTypeCatalog {

	private static final long serialVersionUID = 1L;

	@Override
	public void load() throws PEException {
		// a NULL type is used for some special result sets (i.e. SELECT DATABASE()
		// we shouldn't use this type as the database during CREATE operations
		addType(new MysqlNativeType(MysqlType.NULL).withCharSet(MysqlNativeConstants.MYSQL_CHARSET_UTF8).withUsedInCreate(false));

		// Types are ordered as per JDBC specification - increasing by SQL Data Type
		// LONG NVARCHAR is not a valid Mysql Data Type
//		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.LONG_NVARCHAR).withQuoteCharacter("'").withPrecision(16777215)).withCharSet(
//				MysqlNativeConstants.MYSQL_CHARSET_UTF8).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAGS_NO_FLAGS).withComparable(
//				false));

//		addType(new MysqlNativeType(MysqlType.NCHAR).withQuoteCharacter("'").withPrecision(255).withSupportsPrecision(true));

//		addType(new MysqlNativeType(MysqlType.NVARCHAR).withQuoteCharacter("'").withPrecision(255).withSupportsPrecision(true));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.BIT).withPrecision(1).withMaxPrecision(64).withSupportsPrecision(true)).withCharSet(
				MysqlNativeConstants.MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.BOOL).withPrecision(1).withMaxPrecision(64)).withCharSet(
				MysqlNativeConstants.MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.TINYINT).withPrecision(4).withMaxPrecision(255).withSupportsPrecision(true).withAutoIncrement(
				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.BIGINT).withPrecision(20).withMaxPrecision(255).withSupportsPrecision(true).withAutoIncrement(
				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

//		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.LONG_VARBINARY).withQuoteCharacter("'").withPrecision(16777215)).withCharSet(
//				MYSQL_CHARSET_BINARY).withFieldTypeFlags(
//				MysqlNativeConstants.FLDPKT_FLAGS_BLOB_FLAG + MysqlNativeConstants.FLDPKT_FLAGS_BINARY_FLAG).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.MEDIUMBLOB).withQuoteCharacter("'").withPrecision(16777215)).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.ALTMEDIUMBLOB).withQuoteCharacter("'").withPrecision(16777215)).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.LONGBLOB).withQuoteCharacter("'").withPrecision(4294967295L)).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.ALTLONGBLOB).withQuoteCharacter("'").withPrecision(16777216L)).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.BLOB).withQuoteCharacter("'").withPrecision(65535)).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.TINYBLOB).withQuoteCharacter("'").withPrecision(255)).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.ALTTINYBLOB).withQuoteCharacter("'").withPrecision(255)).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.VARBINARY).withQuoteCharacter("'").withPrecision(255).withSupportsPrecision(
				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.BINARY).withQuoteCharacter("'").withPrecision(255).withSupportsPrecision(
				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

//		addType(new MysqlNativeType(MysqlType.LONG_VARCHAR).withQuoteCharacter("'").withPrecision(16777215).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.MEDIUMTEXT).withQuoteCharacter("'").withPrecision(16777215))
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.LONGTEXT).withQuoteCharacter("'").withPrecision(4294967295L)).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.TEXT).withQuoteCharacter("'").withPrecision(65535)).
				withCharSet(MYSQL_CHARSET_UTF8).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.TINYTEXT).withQuoteCharacter("'").withPrecision(255))
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.CHAR).withQuoteCharacter("'").withPrecision(255).withSupportsPrecision(
				true)).withCharSet(MYSQL_CHARSET_UTF8).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));
// change this to an alias for DECIMAL
//		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.NUMERIC).withPrecision(65).withSupportsPrecision(true).withSupportsScale(
//				true).withAutoIncrement(true).withMinimumScale(-308).withMaximumScale(308)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
//				MysqlNativeConstants.FLDPKT_FLAGS_BINARY_FLAG));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.DECIMAL).withPrecision(65).withSupportsPrecision(true).withSupportsScale(
				true).withAutoIncrement(true).withMinimumScale(-308).withMaximumScale(308)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY));

//		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.INTEGER).withPrecision(11).withMaxPrecision(255).withSupportsPrecision(true).withAutoIncrement(
//				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.INT).withPrecision(11).withMaxPrecision(255).withSupportsPrecision(true).withAutoIncrement(
				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.MEDIUMINT).withPrecision(9).withMaxPrecision(255).withSupportsPrecision(true).withAutoIncrement(
				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.SMALLINT).withPrecision(6).withMaxPrecision(255).withSupportsPrecision(true).withAutoIncrement(
				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.FLOAT).withPrecision(10).withMaxPrecision(53).withSupportsPrecision(true).withSupportsScale(
				true).withAutoIncrement(true).withMinimumScale(-38).withMaximumScale(38)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.DOUBLE).withPrecision(53).withSupportsPrecision(true).withSupportsScale(
				true).withAutoIncrement(true).withMinimumScale(-308).withMaximumScale(308)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.DOUBLE_PRECISION).withPrecision(53).withSupportsPrecision(true).withSupportsScale(
				true).withAutoIncrement(true).withMinimumScale(-308).withMaximumScale(308)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.VARCHAR).withQuoteCharacter("'").withPrecision(65535).withSupportsPrecision(
				true)).withCharSet(MYSQL_CHARSET_UTF8).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_NONE));

		addType(new MysqlNativeType(MysqlType.ENUM).withUsedInCreate(false).withQuoteCharacter("'").withPrecision(65535));
		addType(new MysqlNativeType(MysqlType.SET).withUsedInCreate(false).withQuoteCharacter("'").withPrecision(64));
		
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.DATE).withQuoteCharacter("'")).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.TIME).withQuoteCharacter("'")).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.DATETIME).withQuoteCharacter("'")).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BINARY));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.TIMESTAMP).withQuoteCharacter("'")).withCharSet(
				MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BINARY));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.YEAR).withQuoteCharacter("'").withPrecision(4).withSupportsPrecision(
				true)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BINARY));

		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.PARAMETER).withPrecision(0)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
				MysqlNativeConstants.FLDPKT_FLAG_BINARY));

		/* Spatial types. */
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.GEOMETRY).withQuoteCharacter("'").withPrecision(65535)).withCharSet(MYSQL_CHARSET_BINARY)
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.POINT).withQuoteCharacter("'").withPrecision(65535)).withCharSet(MYSQL_CHARSET_BINARY)
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.LINESTRING).withQuoteCharacter("'").withPrecision(65535)).withCharSet(MYSQL_CHARSET_BINARY)
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.POLYGON).withQuoteCharacter("'").withPrecision(65535)).withCharSet(MYSQL_CHARSET_BINARY)
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.GEOMETRYCOLLECTION).withQuoteCharacter("'").withPrecision(65535)).withCharSet(MYSQL_CHARSET_BINARY)
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.MULTIPOINT).withQuoteCharacter("'").withPrecision(65535)).withCharSet(MYSQL_CHARSET_BINARY)
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.MULTILINESTRING).withQuoteCharacter("'").withPrecision(65535)).withCharSet(MYSQL_CHARSET_BINARY)
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));
		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.MULTIPOLYGON).withQuoteCharacter("'").withPrecision(65535)).withCharSet(MYSQL_CHARSET_BINARY)
				.withFieldTypeFlags(MysqlNativeConstants.FLDPKT_FLAG_BLOB + MysqlNativeConstants.FLDPKT_FLAG_BINARY).withComparable(false));

		// Mysql doesn't support a 'real' type - it's always promoted to 'double'
//		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.REAL).withPrecision(10).withSupportsPrecision(true).withSupportsScale(
//				true).withAutoIncrement(true).withMinimumScale(-38).withMaximumScale(38)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
//				MysqlNativeConstants.FLDPKT_FLAGS_NO_FLAGS).withComparable(false).withUsedInCreate(false));
		//  This is here because we need Types.REAL in the catalog
//		addType(((MysqlNativeType) new MysqlNativeType(MysqlType.REAL_UNUSED).withPrecision(10).withSupportsPrecision(true).withSupportsScale(
//				true).withAutoIncrement(true).withMinimumScale(-308).withMaximumScale(308)).withCharSet(MYSQL_CHARSET_BINARY).withFieldTypeFlags(
//				MysqlNativeConstants.FLDPKT_FLAG_NONE).withComparable(false).withUsedInCreate(false));

		setNumTypesLoaded(typesByName.size());
		sortNativeTypeIdLists();
	}
}
