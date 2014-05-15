// OS_STATUS: public
package com.tesora.dve.db;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.tesora.dve.common.PEBaseTest;
import com.tesora.dve.db.NativeType;

public class NativeTypeTest extends PEBaseTest {

	@Test
	public void testFixNameStringBoolean() {
		try {
			String data[][] = getData();
			for (String[] rec : data) {
				String name = rec[0];
				String expectedTypeName = rec[1];
				String expectedName = rec[2];
				String expectedEnumName = rec[3];
				
				String typeName = NativeType.fixName(name, true);
				assertEquals("Should get correct type name", expectedTypeName, typeName);
				
				String fixedName = NativeType.fixName(name, false);
				assertEquals("Should get correct fixed name", expectedName, fixedName);
				
				fixedName = NativeType.fixName(name);
				assertEquals("Should get correct fixed name through wrapper", expectedName, fixedName);
				
				String enumName = NativeType.fixNameForType(name);
				assertEquals("Should get correct enum name", expectedEnumName, enumName);
			}
		} catch (Exception e) {
			failWithStackTrace(e);
		}
	}

	private String[][] getData() {
		String[][] data = {
				{ null, null, null, null },
				{ "enum", "enum", "enum", "ENUM" },
				{ "ENUM", "enum", "enum", "ENUM" },
				{ "enum ('one', 'two')", "enum", "enum ('one', 'two')", "ENUM" },
				{ "ENUM ('ONE', 'TWO')", "enum", "enum ('ONE', 'TWO')", "ENUM" },
				{ "enum('one', 'two')", "enum", "enum('one', 'two')", "ENUM" },
				{ "ENUM('ONE', 'TWO')", "enum", "enum('ONE', 'TWO')", "ENUM" },
				{ "Long VarChar", "long varchar", "long varchar", "LONG_VARCHAR" },
				{ "InTeger", "integer", "integer", "INTEGER" },
		};
		return data;
	}


}
