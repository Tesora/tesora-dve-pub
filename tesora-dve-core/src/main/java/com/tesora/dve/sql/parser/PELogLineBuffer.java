// OS_STATUS: public
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PELogLineBuffer extends LogLineBuffer {

	// so the format is:
	// DEBUG MMM-DD HH:MM:SS [SSConnectionNNNN] (sql.parser.InvokeParser:89) - <sqlstmt>
	// DEBUG MMM-DD HH:MM:SS [SSConnectionNNNN] (sql.parser.InvokeParser:120) =>
	// errors look like
	// ERROR MMM-DD HH:MM:SS ...
	//
	// So the begin stmt bit is the log header + InvokeParser:89) - 
	// And the end is either the log header + InvokeParser:120) - or the ERROR
	
	private static final String logHeader = "DEBUG .+ \\[SSConnection" + "(\\" + "d+)\\] ";
	private static final String ipHeader = "\\(sql\\.parser\\.InvokeParser:";
	
	private enum Delimiter {

		BEGINPARSE(logHeader + ipHeader + "89\\) - ",true),
		PARSERESULT(logHeader + ipHeader + "120\\) - ",false),
		ERROR("ERROR .+ \\(client\\.messages\\.ResponseMessage:94\\)",false);

		private final Pattern regex;
		private final boolean payload;
		private final String in;
		
		private Delimiter(String pattern, boolean usePayload) {
			in = pattern;
			regex = Pattern.compile(pattern);
			payload = usePayload;
		}		
		public Pattern getRegex() { return regex; }
		public boolean usePayload() { return payload; }
		@SuppressWarnings("unused")
		public String getPattern() { return in; }
	}

	private static final Delimiter[] delimiters = Delimiter.values();

	protected MatchResult[] matches = null;	

	private static class MatchResult {
		
		protected int start;
		protected int end;
		protected Delimiter delimiter;
		protected String connID;
		
		public MatchResult(int startoff, int endoff, Delimiter d, String connectionID) {
			start = startoff;
			end = endoff;
			delimiter = d;
			connID = connectionID;
		}
		
		public Delimiter getDelimiter() {
			return delimiter;
		}
		
		public int getStartOffset() {
			return start;
		}
		
		public int getEndOffset() { 
			return end;
		}		
		
		public String getConnectionID() {
			return connID;
		}
	}

	private MatchResult findMatch(String stmt, int suboff) {
		int offset = -1;
		int endoff = -1;
		Delimiter delimiter = null;
		Matcher matched = null;
		for(int i = 0; i < delimiters.length; i++) {
			Delimiter d = delimiters[i];
			Matcher m = d.getRegex().matcher(stmt);
			if (m.find(suboff)) {
				int thisoffset = m.start();
				if (offset == -1 || thisoffset < offset) {
					offset = thisoffset;
					endoff = m.end();
					delimiter = delimiters[i];
					if (delimiter.usePayload()) {
						matched = m;
					}
				}
			}
		}
		if (offset == -1)
			return null;
		return new MatchResult(offset, endoff, delimiter, (matched == null ? null : matched.group(1)));
	}
	
	@Override
	protected boolean haveStatement() {
		if (buffer == null)
			return false;
		String stmt = buffer.toString();
		matches = new MatchResult[2];
		MatchResult results = findMatch(stmt, 0);
		if (results != null) {
			matches[0] = results;
			int sucoff = results.getEndOffset(); 
			results = findMatch(stmt,sucoff);
			if (results != null) {
				matches[1] = results;
				return true;			
			}
		}
		matches = null;
		return false;
	}
	
	@Override
	protected TaggedStatement getStatement() {
		if (matches != null) {
			// any statement is between the last match position of the first match and the first match position of the last match.
			// well, there might be some extra cruft in there so search backwards from the start offset  of the second match for
			// a carriage return
			String attempt = buffer.toString().substring(matches[0].getEndOffset(), matches[1].getStartOffset());
			String stmt = attempt;
			// the new buffer starts just before the second match
			String newbuf = buffer.toString().substring(matches[1].getStartOffset());
			buffer = new StringBuilder();
			buffer.append(newbuf);
			if (stmt.charAt(0) == '#' || stmt.indexOf("SHOW PROCESSLIST") > -1) {
				// not using it
				return null;
			}
			if (matches[0].getDelimiter().usePayload()) {
				String connID = matches[0].getConnectionID();
				int id = -1;
				if (connID != null) try {
					id = Integer.parseInt(connID);
				} catch (NumberFormatException nfe) {
					System.err.println("Unable to pick conn id out of " + connID);
					id = -1;				
				}
				return new TaggedStatement(stmt,id,null);
			}
		}
		return null;
	}

}
