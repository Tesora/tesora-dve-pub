package com.tesora.dve.distribution;

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

import org.junit.Test;

import static org.junit.Assert.*;

import com.tesora.dve.distribution.RangeLimit;
import com.tesora.dve.exceptions.PEException;

public class GenerationKeyRangeTest {

	@Test
	public void test() throws PEException {
		String s;
		
		RangeLimit rl = new RangeLimit();
		rl.add(new Integer(1));
		s = rl.toString();
//		System.out.println(s);
		assertEquals(rl, RangeLimit.parseLimit(s));
		
		rl.add(new String("Hello"));
		s = rl.toString();
//		System.out.println(s);
		assertEquals(rl, RangeLimit.parseLimit(s));
		
//		rl.add(new byte[] { 0x10, 55, 68 });
//		s = rl.toString();
//		System.out.println(s);
//		assertEquals(rl, RangeLimit.parseLimit(s));
		
		rl.add(new Double(18.32));
		s = rl.toString();
//		System.out.println(s);
		assertEquals(rl, RangeLimit.parseLimit(s));
	}

}
