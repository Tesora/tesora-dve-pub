// OS_STATUS: public
package com.tesora.dve.sql.util;

import static org.junit.Assert.*;

import org.junit.Test;

import com.tesora.dve.sql.util.ResizableArray;

public class ResizableArrayTest {

	@Test
	public void basicTest() {
		ResizableArray<String> stringArray = new ResizableArray<String>();
		
		for (int i = 0; i < 1000; i++) {
			stringArray.set(i, "item " + i);
		}
		
		assertEquals(1000, stringArray.size());
		
		for (int i = 0; i < 1000; i++) {
			assertEquals("failing for index " + i, "item "+i, stringArray.get(i));
		}
		
		// test retrieval past the end
		assertNull(stringArray.get(10000));
	}
	
	@Test
	public void boundaryTest() {
		int[] testData = { 1, 2, 5, 100, 1000, 100000 };
		
		ResizableArray<Integer> intArray = new ResizableArray<Integer>(2);

		assertEquals(0, intArray.size());
		for (int i = 0; i < testData.length; i++) {
			intArray.set(testData[i], new Integer(testData[i] * 11));
			assertEquals("index is " + i, testData[i]+1, intArray.size());
		}

		for (int i = 0; i < testData.length; i++) {
			assertEquals(new Integer(testData[i] * 11), intArray.get(testData[i]));
		}
	}

	@Test
	public void pe764Test() {
		ResizableArray<String> stringArray = new ResizableArray<String>();

		stringArray.set(1, "item 1");
		stringArray.set(11, "item 11");
		stringArray.set(28, "item 28");
		
		assertEquals("item 1", stringArray.get(1));
		assertEquals("item 11", stringArray.get(11));
		assertEquals("item 28", stringArray.get(28));
	}
}
