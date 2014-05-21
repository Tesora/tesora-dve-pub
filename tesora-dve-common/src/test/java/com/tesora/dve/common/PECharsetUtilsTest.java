// OS_STATUS: public
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



import static junitx.framework.ArrayAssert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.common.PECharsetUtils;

public class PECharsetUtilsTest {

	@Test
	public void encodeDecodetest() throws Exception {
		Charset csUTF8 = PECharsetUtils.UTF_8;
		Charset cs8859 = PECharsetUtils.ISO_8859_1;
		
		byte[] utf8Bytes = { (byte) 0xe3, (byte) 0x85, (byte) 0x85, (byte) 0x65, (byte) 0xC7, (byte) 0x90, (byte) 0xd8, (byte) 0xb9, (byte) 0xF0, (byte) 0x9F, (byte) 0x91, (byte) 0x8A };
		String utf8Str = PECharsetUtils.getString(utf8Bytes, csUTF8);
		byte[] newUtf8Bytes = PECharsetUtils.getBytes(utf8Str, csUTF8);
		assertEquals(utf8Bytes, newUtf8Bytes);
		

		byte[] latin1Bytes = { (byte) 0x30, (byte) 0x31, (byte) 0x32, (byte) 0x33 };
		String latin1Str = PECharsetUtils.getString(latin1Bytes, cs8859);
		byte[] newLatin1Bytes = PECharsetUtils.getBytes(latin1Str, cs8859);
		assertEquals(latin1Bytes, newLatin1Bytes);
		
		byte[] badUtf8Bytes = { (byte) 0xf1 };
		assertNull(PECharsetUtils.getString(badUtf8Bytes, csUTF8, true));
		assertNull(PECharsetUtils.getBytes(utf8Str, cs8859, true));
	
		assertTrue(new String("").equals(PECharsetUtils.getString(new byte[0], cs8859,true)));
	}
	
	@Ignore
	@Test
	public void blobTest() throws Exception {
		Charset cs = PECharsetUtils.UTF_8;
		for ( int i = 0; i<255; i++ ) {
			ByteBuffer bb = ByteBuffer.allocate(2);
			byte b = (byte) i;
			if (b == '\\' || b == '\'') {
				byte p = '\\';
				bb.put(p);
			}
			bb.put((byte) i);
			
			System.out.println("Trying " + new String(bb.array(),cs));
			if ( PECharsetUtils.getString(bb.array(), cs, true) == null )
				System.out.println("************** Failed on character " + i);
		}
	}
}
