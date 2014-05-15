// OS_STATUS: public
package com.tesora.dve.tools;

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
