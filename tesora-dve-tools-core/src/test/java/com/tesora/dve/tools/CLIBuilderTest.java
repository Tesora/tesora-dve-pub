// OS_STATUS: public
package com.tesora.dve.tools;

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

import java.util.Scanner;

import org.junit.Test;

import com.tesora.dve.common.PEBaseTest;

public class CLIBuilderTest extends PEBaseTest {

	@Test
	@SuppressWarnings("resource")
	public void testScanFilePath() {
		testScanFilePath(new Scanner("/var/Compile-Test Build/workspace/policy1.xml"),
				"/var/Compile-Test", "Build/workspace/policy1.xml");

		testScanFilePath(new Scanner("'/var/Compile-Test Build/workspace/policy1.xml'"),
				"/var/Compile-Test Build/workspace/policy1.xml", null);

		testScanFilePath(new Scanner("\"/var/Compile-Test Build/workspace/policy1.xml\""),
				"/var/Compile-Test Build/workspace/policy1.xml", null);

		testScanFilePath(new Scanner("'/var/Compile-Test Build/workspace/policy1.xml' extra"),
				"/var/Compile-Test Build/workspace/policy1.xml", "extra");

		testScanFilePath(new Scanner("'/var/Compile-Test Build/workspace/policy1.xml\" extra'"),
				"/var/Compile-Test Build/workspace/policy1.xml\" extra", null);
	}

	private void testScanFilePath(final Scanner scanner, final String expectedPath, final String nextExpectedToken) {
		try {
			final String actualPath = CLIBuilder.scanFilePath(scanner);
			assertEquals(expectedPath, actualPath);
			if (nextExpectedToken != null) {
				assertTrue("More tokens expected, but the scanner is empty.", scanner.hasNext());
				assertEquals(nextExpectedToken, scanner.next());
			} else {
				assertFalse("The scanner has unexpected extra tokens.", scanner.hasNext());
			}
		} finally {
			scanner.close();
		}
	}

}
