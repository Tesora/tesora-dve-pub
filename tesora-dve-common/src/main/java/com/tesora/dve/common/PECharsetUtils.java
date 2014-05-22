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

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;

public final class PECharsetUtils {
	
	public static final Charset UTF_8 = Charset.forName("UTF-8");
	public static final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");
	
	private static ThreadLocal<SoftReference<CharsetDecoder>> decoder = new ThreadLocal<SoftReference<CharsetDecoder>>();
	private static ThreadLocal<SoftReference<CharsetEncoder>> encoder = new ThreadLocal<SoftReference<CharsetEncoder>>();
	public static final Charset latin1 = Charset.forName("ISO-8859-1");

	private PECharsetUtils() {
	}
	
	private static <T> T deref(ThreadLocal<SoftReference<T>> tl) {
		SoftReference<T> sr = (SoftReference<T>) tl.get();
		if (sr == null)
			return null;
		return sr.get();
	}

	private static <T> void set(ThreadLocal<SoftReference<T>> tl, T ob) {
		tl.set(new SoftReference<T>(ob));
	}

	public static String getString(byte[] bytesIn, Charset cs) {
		return getString(bytesIn, cs, false);
	}

	public static String getString(byte[] bytesIn, Charset cs, boolean checkIfValid ) {
		if ( checkIfValid )
			return getString(bytesIn, cs, CodingErrorAction.REPORT);
		else
			return getString(bytesIn, cs, CodingErrorAction.REPLACE);
	}

	static String getString(byte[] bytesIn, Charset cs, CodingErrorAction cea) {
		CharsetDecoder cd = (CharsetDecoder) deref(decoder);
		if ((cd == null) || (!cs.name().equals(cd.charset().name()))) {
			cd = cs.newDecoder();
			set(decoder, cd);
		}

		int en = scale(bytesIn.length, cd.maxCharsPerByte());
		char[] charOut = new char[en];
		if (bytesIn.length == 0)
			return new String(charOut);

		CharBuffer cb = CharBuffer.wrap(charOut);
		CoderResult cr = cd.onMalformedInput(cea).onUnmappableCharacter(cea)
				.decode(ByteBuffer.wrap(bytesIn), cb, true);
		if (cr.isMalformed() || cr.isUnmappable())
			charOut = null;

		return ( charOut == null ? null : new String(safeTrim(charOut, cb.position())));
	}
	
	public static byte[] getBytes(String stringIn, Charset cs) {
		return getBytes(stringIn, cs, false);
	}

	public static byte[] getBytes(String stringIn, Charset cs, boolean checkIfValid) {
		if ( checkIfValid )
			return getBytes(stringIn, cs, CodingErrorAction.REPORT);
		else
			return getBytes(stringIn, cs, CodingErrorAction.REPLACE);
	}

	static byte[] getBytes(String stringIn, Charset cs, CodingErrorAction cea) {
		CharsetEncoder ce = (CharsetEncoder) deref(encoder);
		if ((ce == null) || (!cs.name().equals(ce.charset().name()))) {
			ce = cs.newEncoder();
			set(encoder, ce);
		}

		int en = scale(stringIn.length(), ce.maxBytesPerChar());
		byte[] bytesOut = new byte[en];
		if (stringIn.length() == 0)
			return bytesOut;

		ByteBuffer bb = ByteBuffer.wrap(bytesOut);
		CoderResult cr = ce.onMalformedInput(cea).onUnmappableCharacter(cea)
				.encode(CharBuffer.wrap(stringIn), bb, true);
		if (cr.isMalformed() || cr.isUnmappable())
			bytesOut = null;
		return safeTrim(bytesOut, bb.position());
	}
	
	private static byte[] safeTrim(byte[] ba, int len) {
		if ( ba  == null)
			return null;
		if (len == ba.length)
			return ba;
		else
			return Arrays.copyOf(ba, len);
	}

	private static char[] safeTrim(char[] ca, int len) {
		if ( ca  == null)
			return null;
		if (len == ca.length)
			return ca;
		else
			return Arrays.copyOf(ca, len);
	}

	private static int scale(int len, float expansionFactor) {
		return (int) (len * (double) expansionFactor);
	}

}
