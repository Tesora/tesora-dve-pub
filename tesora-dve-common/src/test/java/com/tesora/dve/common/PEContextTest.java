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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.tesora.dve.common.PEContext;
import com.tesora.dve.common.PEContext.Frame;

public class PEContextTest {

	@Test
	public void testFrameString() throws Exception {
		Frame frame = new Frame("Test");
		assertThat(frame.toString(), containsString("Test"));

		frame.put("a1", "one");
		frame.put("b2", "two");
		assertThat(frame.toString(), allOf(
				containsString("Test"),
				containsString("a1=one"),
				containsString("b2=two")));
		frame.remove("b2");
		assertThat(frame.toString(), not(containsString("b2=two")));
	}

	@Test
	public void testContextString() throws Exception {
		PEContext context = new PEContext();
		assertThat(context.toString(), containsString("PEContext"));

		context.put("a1", "one");
		context.put("b2", "two");
		assertThat(context.toString(), allOf(
				containsString("a1=one"),
				containsString("b2=two")));
	}

	@Test
	public void testPutObjects() throws Exception {
		PEContext context = new PEContext();
		context.put("a1", null);
		context.put("b2", new Integer(2));
		context.put("c3", new Double(3.3));
		assertThat(context.toString(), allOf(
				containsString("a1=null"),
				containsString("b2=2"),
				containsString("c3=3.3")));
	}
	
	@Test
	public void testFrameCopy() throws Exception {
		Frame frame = new Frame("Test");
		frame.put("a1", "one");
		frame.put("b2", "two");

		Frame copy = frame.copy();
		assertEquals(frame.get("a1"), copy.get("a1"));
		assertEquals(frame.get("b2"), copy.get("b2"));

		frame.put("foo", "frame");
		assertEquals("frame", frame.get("foo"));
		assertNull(copy.get("foo"));
		copy.put("foo", "copy");
		assertEquals("frame", frame.get("foo"));
		assertEquals("copy", copy.get("foo"));
	}

	@Test
	public void testContextFrames() throws Exception {
		PEContext context = new PEContext();

		context.put("a1", "one");
		assertEquals(1, context.depth());

		context.push("Frame2");
		context.put("a1", "replaced");
		context.put("b2", "two");

		context.push("Frame3");
		context.put("c3", "three");
		assertEquals(3, context.depth());
		context.pop();

		assertEquals(2, context.depth());
		assertEquals("replaced", context.get("a1"));
		assertEquals("two", context.get("b2"));
		assertNull(context.get("c3"));

		assertThat(context.toString(), allOf(
				containsString("a1=one"),
				containsString("a1=replaced"),
				containsString("b2=two"),
				not(containsString("c3=three"))));
	}

	@Test
	public void testContextCopy() throws Exception {
		PEContext context = new PEContext();
		context.put("a", "A");
		context.push("Two");
		context.put("b", "B");

		PEContext copy = context.copy();
		PEContext copy2 = context.copy();

		assertEquals(context.depth(), copy.depth());
		assertEquals(context.get("a"), copy.get("a"));
		assertEquals(context.get("b"), copy.get("b"));

		copy.put("d", "D");
		assertEquals("D", copy.get("d"));
		assertNull(context.get("d"));

		context.put("e", "E");
		assertEquals("E", context.get("e"));
		assertNull(copy.get("e"));
		assertNull(copy2.get("e"));
	}

}
