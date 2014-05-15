// OS_STATUS: public
package com.tesora.dve.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.tesora.dve.common.PELongUtils;

public class PELongUtilsTest extends PEBaseTest {

	@Test
	public void testDecode() {
		// number, valid decode, decode equivalent if different
		String[][] data = {
				{ "0", "true" },
				{ "-0", "true" },
				{ "+0", "true" },

				{ "00", "true" },
				{ "-00", "true" },
				{ "+00", "true" },

				{ "-10", "true" },
				{ "10", "true" },
				{ "+10", "true" },

				{ "-010", "true", "-10" },
				{ "010", "true", "10" },
				{ "+010", "true", "+10" },

				{ "-80", "true" },
				{ "80", "true" },
				{ "+80", "true" },

				{ "-080", "true", "-80" },
				{ "080", "true", "80" },
				{ "+080", "true", "+80" },

				{ "-0789", "true", "-789" },
				{ "0789", "true", "789" },
				{ "+0789", "true", "+789" },

				{ "-0x01", "true" },
				{ "0x01", "true" },
				{ "+0x01", "true" },

				{ "-0x00", "true" },
				{ "0x00", "true" },
				{ "+0x00", "true" },

				{ "-0x100", "true" },
				{ "0x100", "true" },
				{ "+0x100", "true" },

				{ "-#01", "false" },
				{ "#01", "false" },
				{ "+#01", "false" },

				{ "-#00", "false" },
				{ "#00", "false" },
				{ "+#00", "false" },

				{ "-#100", "false" },
				{ "#100", "false" },
				{ "+#100", "false" },
		};

		int count = 0;
		for (String[] record : data) {
			count++;
			String numberStr = record[0];
			boolean expected = Boolean.parseBoolean(record[1]);
			boolean valid = true;
			try {
				Long longValue = PELongUtils.decode(numberStr);
				String decodeStr = numberStr;
				if (record.length > 2) {
					decodeStr = record[2];
				}
				try {
					Long expectedValue = Long.decode(decodeStr);
					assertEquals("(Rec: " + count + ") Value '" + numberStr + "' should decode correctly",
							expectedValue, longValue);
				} catch (NumberFormatException e) {
					fail("Possible test error - Long.decode cannot parse '" + decodeStr + "'");
				}
			} catch (NumberFormatException e) {
				valid = false;
			}
			assertEquals("(Rec: " + count + ") Value '" + numberStr + "' should have the right parsability",
					expected, valid);
		}
	}

}
