// OS_STATUS: public
package com.tesora.dve.sql.parser;

public class LogLineBuffer {
	
	protected StringBuilder buffer;
	
	public LogLineBuffer() {
		buffer = null;
	}
	
	protected void add(char c, boolean eol) {
		if (buffer == null)
			buffer = new StringBuilder();
		buffer.append(c);
	}

	protected boolean haveStatement() {
		if (buffer != null) {
			String stmt = buffer.toString();
			return complete(stmt);
		}
		return false;
	}

	protected String getRawBuffer() {
		if (buffer == null) return null;
		return buffer.toString();
	}
	
	protected TaggedStatement getStatement() {
		if (buffer != null) {
			String stmt = buffer.toString();
			buffer = null;
			if (ignore(stmt))
				return null;
			return new TaggedStatement(stmt);
		}
		return null;
	}
	
	protected boolean ignore(String line) {
		return line.charAt(0) == '#';
	}
	
	protected boolean complete(String line) {
		return line.endsWith(";");
	}
	
	protected void close() {
		// does nothing for default
	}
}