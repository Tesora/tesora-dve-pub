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


import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Locale;

import com.tesora.dve.sql.parser.filter.LogFileFilter;

class MysqlLogLineBuffer extends LogLineBuffer {
	// for mysql log files the format is:
	// whitespace connection-id whitespace command-type whitespace command
	// or
	// integer whitespace time whitespace conn-id whitespace command-type whitespace command
	// where command can take up several lines.  in all cases a new entry always starts
	// with the beginning of the line, then the whitespace, etc.  
	// we used to look for the command and then match around it, but we're switching to a regex since
	// it will likely be easier to avoid false matches.
	
	private static final MysqlLogFileEntryKind[] delimiters = MysqlLogFileEntryKind.values();

	private static class MatchResult {
		
		protected int start;
		protected int end;
		protected MysqlLogFileEntryKind delimiter;
		protected String connID;
		
		public MatchResult(int startoff, int endoff, MysqlLogFileEntryKind d, String connStr) {
			start = startoff;
			end = endoff;
			delimiter = d;
			connID = connStr;
		}
		
		public MysqlLogFileEntryKind getDelimiter() {
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
	
	protected LogFileFilter filter;
		
	public MysqlLogLineBuffer() {
		super();
	}
	
	public MysqlLogLineBuffer(LogFileFilter filter) {
		this();
		this.filter = filter;
	}

	private static final int START = 1;
	private static final int YEAR = 2;
	private static final int HOUR = 3;
	private static final int MINUTE = 4;
	private static final int SECOND = 5;
	private static final int NOTIME = 6;  // expect \t\t
	private static final int CONN = 7;
	private static final int COMMAND = 8;
	private static final int PRECONN_WS = 9; // for mysql 5.1
	private static final int PREHOUR_WS = 10;
	
	private LinkedHashSet<MysqlLogFileEntryKind> possibilities = new LinkedHashSet<MysqlLogFileEntryKind>();
	private int pthumb = -1;
	private int connoffset = -1;
	private String currentConnID = null;
	private int startedAt = -1;
	
	private MatchResult left = null, right = null;
	
	private char[] headerBuf = new char[40];
	private int headerThumb = -1;
	private int state;
	
	private void processStartState(char c) {
		headerBuf[++headerThumb] = c;
		startedAt = buffer.length();
		connoffset = -1;
		if (Character.isDigit(c)) {
			state = YEAR;
		} else if ('\t' == c) {
			state = NOTIME;
		} else {
			// not a start
			headerThumb = -1;
			startedAt = -1;
			state = 0;
		}
	}
	
	private void processNonStartState(char c) {
		// look at the previous
		switch(state) {
		case NOTIME:
			if ('\t' == c) {
				state = PRECONN_WS;
			} else {
				state = 0;
			}
			break;
		case YEAR:
			if (Character.isDigit(c)) {
				// ok
			} else if (' ' == c) {
				state = PREHOUR_WS;
			} else {
				state = 0;
			}
			break;
		case PREHOUR_WS:
			if (' ' == c) {
				// ok
			} else if (Character.isDigit(c)) {
				state = HOUR;
			} else {
				state = 0;
			}
			break;
		case HOUR:
			if (Character.isDigit(c)) {
				// ok
			} else if (':' == c) {
				state = MINUTE;
			} else {
				state = 0;
			}
			break;
		case MINUTE:
			if (Character.isDigit(c)) {
				// ok
			} else if (':' == c) {
				state = SECOND;
			} else {
				state = 0;
			}
			break;
		case SECOND:
			if (Character.isDigit(c)) {
				// ok
			} else if ('\t' == c || ' ' == c) {
				state = PRECONN_WS;
			} else {
				state = 0;
			}
			break;
		case PRECONN_WS:
			if ('\t' == c || ' ' == c) {
				// ok
			} else if (Character.isDigit(c)) {
				state = CONN;
				if (connoffset == -1)
					connoffset = headerThumb+1;
			} else {
				state = 0;
			}
			break;
		case CONN:
			if (Character.isDigit(c)) {
				// ok
				if (connoffset == -1)
					connoffset = headerThumb+1;
			} else if (' ' == c && connoffset != -1) {
				// it's unclear when we enter the conn state and don't set connoffset, but if that is the case, we are confused.
				// move on.
				//
				// figure out the current conn value now
				currentConnID = new String(headerBuf,connoffset,(headerThumb - connoffset + 1));
				state = COMMAND;
				pthumb = -1;
			} else {
				state = 0;
			}
			break;
		case COMMAND:
			handleCommand(c);
			break;
		}
		if (state != 0) {
			headerBuf[++headerThumb] = c;
		} else {
			// reset all
			headerThumb = -1;
			pthumb = -1;
			currentConnID = null;
			connoffset = -1;
			if (right != null && right.getDelimiter() == MysqlLogFileEntryKind.QUIT) {
				state = START;
			}
		}
	}
	
	private void handleCommand(char c) {
		if (pthumb == -1) {
			possibilities.clear();
			// first character, we have to build the set
			for(MysqlLogFileEntryKind m : delimiters) {
				if (c == m.getMatch().charAt(0)) {
					possibilities.add(m);
				}
			}
			if (possibilities.isEmpty()) {
				state = 0;
			} else {
				pthumb = 1;
			}
		} else if ((c == '\t') || (c == '\n')) {
			// possibly this means we have the whole thing
			// if we have possibilities left that are longer than pthumb - get rid of them
			for(Iterator<MysqlLogFileEntryKind> iter = possibilities.iterator(); iter.hasNext(); ) {
				MysqlLogFileEntryKind m = iter.next();
				if (m.getMatch().length() > pthumb)
					iter.remove();
			}
			if (possibilities.size() > 1)
				state = 0;
			else {
				MysqlLogFileEntryKind currentEntry = possibilities.iterator().next();
				MatchResult mr = new MatchResult(startedAt,buffer.length(),currentEntry,currentConnID);
				if (left == null)
					left = mr;
				else if (right == null)
					right = mr;
				// we've identified, state is 0
				state = 0;
			}
		} else {
			for(Iterator<MysqlLogFileEntryKind> iter = possibilities.iterator(); iter.hasNext();) {
				MysqlLogFileEntryKind m = iter.next();
				if (pthumb < m.getMatch().length() && c == m.getMatch().charAt(pthumb)) {
					// ok
				} else {
					iter.remove();
				}
			}
			if (possibilities.isEmpty()) {
				state = 0;
			} else {
				pthumb++;
			}
		}
	}
	
	@Override
	protected void add(char c, boolean eol) {
		if (buffer == null)
			buffer = new StringBuilder();
		buffer.append(c);
		if (eol && !isHandlingCommand()) {
			state = START;
		} else if (state == START) {
			processStartState(c);
		} else if (state != 0) {
			processNonStartState(c);
		}
	}

	protected boolean haveStatement() {
		if (buffer == null || left == null || right == null)
			return false;
		return true;
	}
	
	@Override
	protected TaggedStatement getStatement() {
		if (left != null && right != null) {
			// any statement is between the last match position of the first match and the first match position of the last match.
			// well, there might be some extra cruft in there so search backwards from the start offset  of the second match for
			// a carriage return
			String attempt = buffer.toString().substring(left.getEndOffset(), right.getStartOffset()+1);
			char[] arr = attempt.toCharArray();
			int last = arr.length - 1;
			for(int i = arr.length - 1; i > -1; i--) {
				char c = arr[i];
				if (c == '\n') {
					last = i;
					break;
				}
			}
			String stmt = attempt.substring(0,last + 1);
			// the connection id is just before the first position in stmt - so search backwards from queryPositions[0]
			// reset the buffer to just before the last Query
			int connID = -1;
			try {
				connID = Integer.parseInt(left.getConnectionID());
			} catch (NumberFormatException nfe) {
				System.err.println("Unable to pick conn id out of " + left.getConnectionID());
				connID = -1;				
			}
			
			if (left.getDelimiter() == MysqlLogFileEntryKind.QUIT) {
				stmt = MysqlLogFileEntryKind.QUIT.name();
			}
			
			// the new buffer starts just before the second match
			String newbuf = buffer.toString().substring(right.getStartOffset());
			buffer = new StringBuilder();
			buffer.append(newbuf);
			MatchResult oldLeft = left;
			// we have to rejigger right and set it to left
			left = new MatchResult(0,right.getEndOffset() - right.getStartOffset(),right.getDelimiter(),right.getConnectionID());
			right = null;
			
			if (stmt.charAt(0) == '#') {
				// not using it
				return null;
			} else if (oldLeft.getDelimiter().ignore()) {
				return null;
			}
			if (filterCurrentLine(connID, stmt)) {
				return null;
			}
			if (oldLeft.getDelimiter().usePayload()) {
				if (oldLeft.getDelimiter() == MysqlLogFileEntryKind.INITDB) {
					// initdb is basically a use statement - make it look that way
					return new TaggedStatement("use " + stmt,connID,oldLeft.getDelimiter());
				} else if (oldLeft.getDelimiter() == MysqlLogFileEntryKind.CONNECT) {
					if (stmt.toLowerCase(Locale.ENGLISH).startsWith("access denied")) {
						return null;
					} else if (stmt.toLowerCase(Locale.ENGLISH).indexOf(" on ") > 0) {
						// if the line ends with "on <word>" then the word is the db
						int offset = stmt.toLowerCase(Locale.ENGLISH).indexOf(" on ") + 4;
						char[] buf = stmt.substring(offset).toCharArray();
						StringBuilder dbname = new StringBuilder();
						for(int i = 0; i < buf.length; i++) {
							if (Character.isWhitespace(buf[i])) {
								break;
							} else {
								dbname.append(buf[i]);
							}
						}
						String db = dbname.toString();
						if ("".equals(db))
							return new TaggedStatement("Connect", connID,oldLeft.getDelimiter());

						return new TaggedStatement("use " + db, connID,oldLeft.getDelimiter());
					} else if (stmt.toLowerCase(Locale.ENGLISH).endsWith(" on")) {
						// empty connect in this case, treat like a regular connect
						return new TaggedStatement("Connect", connID,oldLeft.getDelimiter());
					}
				} else if (oldLeft.getDelimiter() == MysqlLogFileEntryKind.QUERY) {
					if (stmt.startsWith("-")) {
						// potentially, we're going to fail it, so toss it overboard
						return null;
					}
				}
				return new TaggedStatement(stmt,connID,oldLeft.getDelimiter());
			} else {
				return new TaggedStatement(oldLeft.getDelimiter().getMatch(), connID,oldLeft.getDelimiter());
			}
		}
		return null;
	}
	
	boolean filterCurrentLine(int connID, String stmt) {
		if (filter == null) {
			return false;
		} else {
			return filter.filterLine(connID, stmt);
		}
	}
	
	private boolean isHandlingCommand() {
		return pthumb != -1;
	}
}