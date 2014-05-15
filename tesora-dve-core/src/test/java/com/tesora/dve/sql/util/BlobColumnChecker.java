// OS_STATUS: public
package com.tesora.dve.sql.util;

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