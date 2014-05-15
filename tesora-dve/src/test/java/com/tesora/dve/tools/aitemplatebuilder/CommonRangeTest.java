// OS_STATUS: public
package com.tesora.dve.tools.aitemplatebuilder;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.tesora.dve.common.PEBaseTest;

public class CommonRangeTest extends PEBaseTest {

	@Test
	public void testFindPositionFor() {
		final SortedSet<Integer> values = new TreeSet<Integer>(Arrays.asList(-4, -3, 0, 2, 5, 13, 14, 15, 16, 17, 18, 20));

		assertEquals(0, CommonRange.findPositionFor(-6, values));
		assertEquals(0, CommonRange.findPositionFor(-5, values));
		assertEquals(0, CommonRange.findPositionFor(-4, values));
		assertEquals(1, CommonRange.findPositionFor(-3, values));
		assertEquals(2, CommonRange.findPositionFor(-2, values));
		assertEquals(2, CommonRange.findPositionFor(0, values));
		assertEquals(4, CommonRange.findPositionFor(4, values));
		assertEquals(5, CommonRange.findPositionFor(12, values));
		assertEquals(5, CommonRange.findPositionFor(13, values));
		assertEquals(6, CommonRange.findPositionFor(14, values));
		assertEquals(10, CommonRange.findPositionFor(18, values));
		assertEquals(11, CommonRange.findPositionFor(19, values));
		assertEquals(11, CommonRange.findPositionFor(20, values));
		assertEquals(12, CommonRange.findPositionFor(21, values));
	}

}
