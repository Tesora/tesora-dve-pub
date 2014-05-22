package com.tesora.dve.sql.util;

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

import io.netty.util.CharsetUtil;

public class BlobColumnChecker extends ColumnChecker {
	
	private boolean useFormatedOutput = true;

	public BlobColumnChecker() {
		super();
	}

	public void useFormatedOutput(final boolean enable) {
		this.useFormatedOutput = enable;
	}

	@Override
	public String asString(Object in) {
		if (in == null) {
			return "null";
		}

		// May easily lead to heap exhaustion when handling large result sets.
		if (useFormatedOutput) {
			return asFormattedString(in);
		}

		return new String((byte[]) in, CharsetUtil.ISO_8859_1);
	}

	private String asFormattedString(Object in) {
		byte[] bytes = (byte[]) in;
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < bytes.length; i++) {
			buf.append(i).append("='").append(bytes[i]).append("' ");
		}

		return buf.toString();
	}

	@Override
	public String debugString(Object in) throws Throwable {
		if (in == null)
			return asString(in);
		byte[] bytes = (byte[]) in;
		String iso = new String(bytes, "ISO-8859-1");
		return asString(in) + ": \"" + iso + "\"";
	}
	
	@Override
	protected String equalObjects(Object expected, Object actual) {
		byte[] ein = (byte[]) expected;
		byte[] ain = (byte[]) actual;
		if (ein.length != ain.length)
			return "Expected byte array of length " + ein.length + " but found length " + ain.length;
		for(int i = 0; i < ein.length; i++) {
			if (ein[i] != ain[i])
				return "At index " + i + " of byte array expected '" + ein[i] + "' but found '" + ain[i] + "'";
		}
		return null;
	}

}