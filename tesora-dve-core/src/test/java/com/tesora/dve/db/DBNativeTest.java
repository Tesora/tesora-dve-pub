// OS_STATUS: public
package com.tesora.dve.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.junit.Test;

import com.tesora.dve.common.DBType;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.NativeTypeAlias;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.exceptions.PEException;

public class DBNativeTest {

	@Test(expected = PEException.class)
	public void testInvalidDBType() throws PEException {
		DBNative.DBNativeFactory.newInstance(DBType
				.fromDriverClass("unknown.driver.class"));
	}

	@Test
	public void testNativeType() {
		// construct a "fake" generic type to exercise all of the set methods
		NativeType type = new GenericNativeType("MYTYPE", MyFieldType.FIELD_TYPE_STRING, Types.CHAR, false, new NativeTypeAlias[] {
				new NativeTypeAlias("MY_TYPE"), new NativeTypeAlias("TYPE_MY", true) });
		type.withAutoIncrement(false).withCaseSensitive(true)
				.withCreateParams("Params").withLiteralPrefix("'")
				.withPrecision(255).withLiteralSuffix("'").withMaximumScale(30)
				.withMinimumScale(1).isNullable(true).withNumPrecRadix(10)
				.withSearchable(DatabaseMetaData.typeSearchable)
				.withUnsignedAttribute(false).withSupportsPrecision(false)
				.withSupportsScale(false).withComparable(true);

		assertEquals("mytype", type.getTypeName());
		assertEquals(Types.CHAR, type.getDataType());
		assertFalse(type.isJdbcType());
		assertFalse(type.isAutoIncrement());
		assertTrue(type.isCaseSensitive());
		assertEquals("Params", type.getCreateParams());
		assertEquals("'", type.getLiteralPrefix());
		assertEquals("'", type.getLiteralSuffix());
		assertEquals(255, type.getPrecision());
		assertEquals(30, type.getMaximumScale());
		assertEquals(1, type.getMinimumScale());
		assertTrue(type.isNullable());
		assertEquals(DatabaseMetaData.typeNullable, type.getNullability());
		assertEquals(10, type.getNumPrecRadix());
		assertEquals(DatabaseMetaData.typeSearchable, type.getSearchable());
		assertFalse(type.isUnsignedAttribute());
		assertFalse(type.getSupportsPrecision());
		assertFalse(type.getSupportsScale());
		assertTrue(type.isComparable());
	}

	class GenericNativeType extends NativeType {
		private static final long serialVersionUID = 1L;

		public GenericNativeType(String typeName, MyFieldType mft, int dataType, boolean jdbcType, NativeTypeAlias[] aliases) {
			super(typeName, mft.getByteValue(), dataType, jdbcType, aliases);
		}
	}

}
