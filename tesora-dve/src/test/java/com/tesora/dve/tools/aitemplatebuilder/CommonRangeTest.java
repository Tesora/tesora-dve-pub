// OS_STATUS: public
package com.tesora.dve.tools.aitemplatebuilder;

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
