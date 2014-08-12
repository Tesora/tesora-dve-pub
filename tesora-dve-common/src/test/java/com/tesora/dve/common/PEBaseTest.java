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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.tesora.dve.exceptions.PEException;

/**
 * Base class for all Tesora DVE tests.  Sets up things like logging and temporary
 * work directories.  Useful when no DVE specific infrastructure is required
 *
 */
public abstract class PEBaseTest {
	
	protected static class LargeTestResourceNotAvailableException extends PEException {

		private static final long serialVersionUID = -1629221497003489808L;

		public LargeTestResourceNotAvailableException(final String m) {
			super(m);
		}
		
	}
	
	private static final String BT_PROPERTIES_FILENAME = "local.properties";
	
	protected static final String LARGE_RESOURCE_DIR_VAR = "parelastic.test.resourcedir";
	protected static String applicationName = "Unknown";
	protected static String workDir = null;
	protected static boolean deleteWorkDirectoryAfterEachTest = false;
	protected static Logger logger = Logger.getLogger(PEBaseTest.class);

	private static Properties localProps = getPersonalProperties();
	private static final Properties getPersonalProperties()
	{
		Properties props = new Properties();
		try
		{
			props.load(new FileInputStream(BT_PROPERTIES_FILENAME));
		}
		catch (Exception e)
		{
			// ignore if we can't load the file
		}
		return props;
	}

	@org.testng.annotations.BeforeClass
	public void beforeClassTestNG()
	{
		System.out.println("Running " + this.getClass().getName());
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// do nothing
	}

	@AfterClass
	public static void tearDownBaseAfterClass() throws Exception
	{
		// reset variables
		applicationName = "Unknown";
		workDir = null;
		deleteWorkDirectoryAfterEachTest = false;
	}
	
	protected static File getFileFromLargeFileRepository(final String fileName) throws LargeTestResourceNotAvailableException {
		final String largeFileRepositoryPath = System.getProperty(LARGE_RESOURCE_DIR_VAR);
		if (largeFileRepositoryPath == null) {
			throw new LargeTestResourceNotAvailableException("Environment variable '" + LARGE_RESOURCE_DIR_VAR + "' is undefined.");
		}

		final File returnFile = new File(new File(largeFileRepositoryPath), fileName);
		if (!returnFile.canRead()) {
			throw new LargeTestResourceNotAvailableException("The file '" + returnFile.getAbsolutePath() + "' is not accessible.");
		}

		return returnFile;
	}

	/**
	 * Creates a new randomly named directory in the default temporary-file directory.
	 * The directory gets deleted when the virtual machine terminates.
	 */
	protected static File getTemporaryDirectory() throws IOException {
		final File temp = Files.createTempDirectory(null).toFile();
		temp.deleteOnExit();
		return temp;
	}
	
	/**
	 * This class provides an interface for testing expected exception thrown in tests.
	 * Make a new instance of this class overriding the test() method
	 * where you specify the tested code. You then call assertException() on this object giving it
	 * the exception class you expect to be thrown from the tested block.
	 */
	protected static abstract class ExpectedExceptionTester {
		
		private static enum Event {
			WRONG_TYPE, WRONG_MSG, OK;
		}

		public void assertException(final Class<? extends Throwable> expectedExceptionClass) {
			assertException(expectedExceptionClass, null, false);
		}

		public void assertException(final Class<? extends Throwable> expectedExceptionClass, final String expectedExceptionMessage) {
			assertException(expectedExceptionClass, expectedExceptionMessage, false);
		}
		
		/**
		 * Execute the test.
		 * 
		 * @param expectedExceptionClass
		 *            The Exception class you expect to catch from the tested
		 *            code.
		 * @param expectedExceptionMessage
		 *            The expected message of the given exception.
		 * @param traceCauseTree
		 *            Look down the cause tree until you find the expected
		 *            exception type or hit the end.
		 */
		public void assertException(final Class<? extends Throwable> expectedExceptionClass, final String expectedExceptionMessage, final boolean traceCauseTree) {
			try {
				test();
			} catch (Throwable e) {
				Event result = checkException(e, expectedExceptionClass, expectedExceptionMessage);
				if (traceCauseTree) {
					while ((e.getCause() != null) && (result == Event.WRONG_TYPE)) {
						e = e.getCause();
						result = checkException(e, expectedExceptionClass, expectedExceptionMessage);
					}
				}
				switch (result) {
				case WRONG_TYPE:
					failWithStackTrace(e, "An unexpected exception '" + e.getClass() + "' was thrown.");
					break;
				case WRONG_MSG:
					fail("An exception with wrong message ('" + e.getMessage() + "') was thrown.");
					break;
				case OK:
					return;
				default:
					break;
				}
			}

			fail("Expected exception '" + expectedExceptionClass + "' was not thrown.");
		}
		
		/**
		 * The code to be tested.
		 */
		public abstract void test() throws Throwable;
		
		private Event checkException(final Throwable e, final Class<? extends Throwable> expectedExceptionClass, final String expectedExceptionMessage) {
			if (expectedExceptionClass.isAssignableFrom(e.getClass())) {
				String msg = e.getMessage();
				// strip PEContext, if any
				int ix = (msg != null ? msg.indexOf(PEContext.LOG_PREFIX) : -1);
				if (ix > 0) {
					msg = msg.substring(0, ix).trim();
				}
				if ((expectedExceptionMessage != null) && !msg.equals(expectedExceptionMessage)) {
					return Event.WRONG_MSG;
				} else {
					return Event.OK;
				}
			} else {
				return Event.WRONG_TYPE;
			}
		}

	}
	
	/**
	 * Assert a type for a given object.
	 */
	protected void assertType(final Object object, final Class<?> type) {
		if (!object.getClass().isAssignableFrom(type)) {
			final StringBuilder message = new StringBuilder();
			message.append("Type <").append(type.getName()).append("> expected but was <").append(object.getClass().getName()).append(">.");
			fail(message.toString());
		}
	}

	/**
	 * Assert that two objects are equal. If the objects are arrays compare
	 * them element-wise, using equals(), handling multi-dimensional arrays
	 * correctly.
	 */
	protected static void assertEqualData(final Object expected, final Object actual) {
		if ((expected != null) && expected.getClass().isArray()) {
			assertTrue("expected: <" + ArrayUtils.toString(expected) + "> but was: <" + ArrayUtils.toString(actual) + ">",
					ArrayUtils.isEquals(expected, actual));
		} else {
			assertEquals(expected, actual);
		}
	}

	protected static void failWithStackTrace(Throwable e)
	{
		failWithStackTrace(e, null);
	}

	protected static void failWithStackTrace(Throwable e, String message)
	{
		fail((message != null && !message.isEmpty() ? message : "Should not throw exception") + ": "
				+ e.getLocalizedMessage() + "\nTrace: " + PEStringUtils.toString(e));
	}

	/**
	 * Returns the value for the given label. Checks for properties first, then the environment
	 * 
	 * @param label
	 * @return
	 */
	protected static String determineValue(String label)
	{
		return determineValue(label, null);
	}

	/**
	 * Returns the value for the given label. Checks for properties first, then the environment
	 * 
	 * @param label
	 * @return
	 */
	protected static String determineValue(String label, String defaultValue)
	{
		String value = localProps.getProperty(label.replaceAll("_", ".").toLowerCase());
		if (value == null || value.isEmpty())
		{
			value = localProps.getProperty(label);
		}
		if (value == null || value.isEmpty())
		{
			value = System.getProperty(label.replaceAll("_", ".").toLowerCase());
		}
		if (value == null || value.isEmpty())
		{
			value = System.getProperty(label);
		}
		if (value == null || value.isEmpty())
		{
			value = System.getenv(label);
		}
		if (value == null || value.isEmpty())
		{
			value = defaultValue;
		}
		return value;
	}
	
	/**
	 * Count rows in a given result set.
	 */
	protected int countResultSetRows(final ResultSet resultSet) throws SQLException {
		if (resultSet == null) { throw new IllegalArgumentException(); }
		
		int counter = 0;
		while (resultSet.next()) {
			++counter;
		}
		
		return counter;
	}

}
