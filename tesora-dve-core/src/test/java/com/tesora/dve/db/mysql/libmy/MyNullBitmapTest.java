// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import static org.junit.Assert.*;

import org.junit.Test;

import com.tesora.dve.db.mysql.libmy.MyNullBitmap;
import com.tesora.dve.db.mysql.libmy.MyNullBitmap.BitmapType;
import com.tesora.dve.exceptions.PEException;

public class MyNullBitmapTest {

	@Test
	public void positiveBitmapTest() throws PEException {
		final int NUM_FIELDS = 50;
		
		testBitmap(BitmapType.EXECUTE_REQUEST, NUM_FIELDS, (NUM_FIELDS + 7)/8);
	
		testBitmap(BitmapType.RESULT_ROW, NUM_FIELDS, (NUM_FIELDS + 7 + 2)/8);
		

	}

	@Test
	public void edgeBitmapTest() throws PEException {
		testBitmap(BitmapType.RESULT_ROW, 1, 1);
		testBitmap(BitmapType.EXECUTE_REQUEST, 1, 1);

		testBitmap(BitmapType.RESULT_ROW, 0, 1);
		testBitmap(BitmapType.EXECUTE_REQUEST, 0, 0);

		testBitmap(BitmapType.RESULT_ROW, 7, 2);
		testBitmap(BitmapType.EXECUTE_REQUEST, 7, 1);

		testBitmap(BitmapType.RESULT_ROW, 8, 2);
		testBitmap(BitmapType.EXECUTE_REQUEST, 8, 1);

		testBitmap(BitmapType.RESULT_ROW, 9, 2);
		testBitmap(BitmapType.EXECUTE_REQUEST, 9, 2);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void failBitmapTest1() throws Exception {
		MyNullBitmap nb = new MyNullBitmap(10, BitmapType.EXECUTE_REQUEST);
		nb.getBit(11);
	}

	@Test (expected=IllegalArgumentException.class)
	public void failBitmapTest2() throws Exception {
		MyNullBitmap nb = new MyNullBitmap(0, BitmapType.EXECUTE_REQUEST);
		nb.setBit(11);
	}

	private void testBitmap(BitmapType bt, int numFields, int expBytes) throws PEException {
		MyNullBitmap nb = new MyNullBitmap(numFields, bt);
		assertEquals(numFields, nb.size());
		assertEquals(expBytes, nb.getBitmap().length);
		
		// check none are set
		for (int i = 1; i <= nb.size(); i++) {
			assertFalse("Bit at position " + i + " is set and shouldn't be", nb.getBit(i));
		}

		// set some positions
		for (int i = 1; i <= nb.size(); i++) {
			if ( (i % 2) == 0 )
				nb.setBit(i);
		}
		// check some positions
		for (int i = 1; i <= nb.size(); i++) {
			if ( (i % 2) == 0 )
				assertTrue("Bit at position " + i + " isn't set and should be", nb.getBit(i));
			else
				assertFalse("Bit at position " + i + " is set and shouldn't be", nb.getBit(i));
		}

		// set all positions
		for (int i = 1; i <= nb.size(); i++) {
			nb.setBit(i);
		}

		// check all all set
		for (int i = 1; i <= nb.size(); i++) {
			assertTrue("Bit at position " + i + " isn't set and should be", nb.getBit(i));
		}

	}
	
	@Test public void testAllFlips() throws Exception {
		testAllForType(BitmapType.EXECUTE_REQUEST);
		testAllForType(BitmapType.RESULT_ROW);
	}

	private void testAllForType(BitmapType bitmapType) throws Exception {
		for (int bitmapLength = 1; bitmapLength < 64; ++bitmapLength) {
			MyNullBitmap nullBitmap = new MyNullBitmap(bitmapLength, bitmapType);
			testFlip(nullBitmap, -1, bitmapLength, bitmapType);
			for (int bitToSet = 1; bitToSet <= bitmapLength; ++bitToSet) {
				nullBitmap = new MyNullBitmap(bitmapLength, bitmapType);
				nullBitmap.setBit(bitToSet);
				testFlip(nullBitmap, bitToSet, bitmapLength, bitmapType);
			}
		}
	}

	private void testFlip(MyNullBitmap origNullBitmap, int bitToSet, int bitmapLength, BitmapType bitmapType) throws Exception {
		String assertMessage = "Nullbitmap("+bitmapType+") of " + bitmapLength + " bits with bit " + bitToSet + " set";
		try {
			MyNullBitmap flippedBitmap = origNullBitmap.flipType();
			for (int i = 1; i <= origNullBitmap.size(); ++i) 
				assertEquals(assertMessage + "(checking bit "+i+")", origNullBitmap.getBit(i), flippedBitmap.getBit(i));
		} catch (Exception e) {
			throw new Exception(assertMessage, e);
		}
	}

}
