// OS_STATUS: public
package com.tesora.dve.common;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.tesora.dve.common.PatternLayoutWithHeader;

public class PELogUtilsTest {

	@Test
	public void getBuildTagTest() throws IOException {
		// for the most part, this just tests that an exception isn't thrown
		// as during testing, we aren't reading the MVN generated manifest
		// file so none of the values we need are set...
		PatternLayoutWithHeader plwh = new PatternLayoutWithHeader();
		assertTrue( plwh.getHeader().contains("Developer Build"));
	}
}
