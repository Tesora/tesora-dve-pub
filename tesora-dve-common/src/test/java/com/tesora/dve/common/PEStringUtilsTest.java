// OS_STATUS: public
package com.tesora.dve.common;

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
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Test;

import com.tesora.dve.common.PEStringUtils;

public class PEStringUtilsTest  extends PEBaseTest {

	@Test
	public void normalizePrefixTest() {
		assertEquals("", PEStringUtils.normalizePrefix(null));
		assertEquals("", PEStringUtils.normalizePrefix(""));
		assertEquals("test.", PEStringUtils.normalizePrefix("test"));
		assertEquals("test.", PEStringUtils.normalizePrefix("test."));
	}

	@Test
	public void testGetStackTrace()
	{
		try
		{
			throw new Exception("TestException");
		} catch (Exception e)
		{
			String expected = "java.lang.Exception: TestException";
			String value = PEStringUtils.toString(e);
			assertTrue("Stack trace should start with '" + expected + "' but was '" + value + "'",
					value.startsWith(expected));
		}
	}

	@Test
	public void buildSQLPatternTest() {
		String[][] testValues = {
				// input pattern, true value, false value
				{ "foo", "foo", "fool" },
				{ "%here%", "111here111", "her1e" },
				{ "h%re%", "h111rexxx", "1here" },
				{ "\\%foo", "%foo", "here" },
				{ "__foo", "22foo", "1foo" },
				{ "%\\_%", "has_underscore", "nouscore" },
				{ "???foo", "???foo", "123foo" }
		};

		String msg;
		for (int i = 0; i < testValues.length; i++) {
			msg = "Item " + i + " failed";
			Pattern p = PEStringUtils.buildSQLPattern(testValues[i][0]);
			assertTrue(msg, p.matcher(testValues[i][1]).matches());
			assertFalse(msg, p.matcher(testValues[i][2]).matches());
		}
	}

	@Test
	public void toBooleanTest() {
		// string, true/false
		String[][] testValues = {
				{ "true", "true" },
				{ "True", "true" },
				{ "TRUE", "true" },
				{ "truE", "true" },
				{ "on", "true" },
				{ "On", "true" },
				{ "ON", "true" },
				{ "oN", "true" },
				{ "yes", "true" },
				{ "Yes", "true" },
				{ "YES", "true" },
				{ "yEs", "true" },
				{ "1", "true" },
				{ "11", "true" },
				{ "23", "true" },
				{ "0", "false" },
				{ "-0", "false" },
				{ "-1", "false" },
				{ "-10", "false" },
				{ null, "false" },
				{ "false", "false" },
				{ "False", "false" },
				{ "faLse", "false" },
				{ "falsely", "false" },
				{ "off", "false" },
				{ "Off", "false" },
				{ "OFF", "false" },
				{ "oFf", "false" },
				{ "offfff", "false" },
				{ "Yessir", "false" },
				{ "YESmostly", "false" },
		};

		for (String[] testValue : testValues) {
			String string = testValue[0];
			boolean expected = Boolean.parseBoolean(testValue[1]);
			assertEquals("Should find the right 'boolean' for value '" + string + "'",
					expected, PEStringUtils.toBoolean(string));
		}
	}

}
