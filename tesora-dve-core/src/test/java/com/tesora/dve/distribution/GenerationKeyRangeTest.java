// OS_STATUS: public
package com.tesora.dve.distribution;

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
