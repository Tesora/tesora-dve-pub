package com.tesora.dve.sql.parser;

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