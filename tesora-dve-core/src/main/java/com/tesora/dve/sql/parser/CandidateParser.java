// OS_STATUS: public
package com.tesora.dve.sql.parser;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.transform.execution.ExecutionType;

// this is not really a parser
// what we're going to do is scan the input string left to right and build two objects:
// [1] a query with all literals removed and replaced with parameters
// [2] a list of literals
//
// [1] has to be sufficiently unique to be a good cache key
// [2] has to be the same as we would get from the full parser
//
// The rules are - numbers can only start if previous is nonletter
// numbers are composed of digits + '.'
// String literals are enclosed in single quotes, and previous cannot be a letter to start
//    (note that this means no hex literals for now, but so what)
// When accumulating a string literal, keep track of escaped single quotes
//
// the purpose of this thing is to be really fast - so if we get confused just give up
// we gain if we handle 80% of the cases, we don't need to handle all of them
public class CandidateParser {

	private String in;
	private String shrunk;
	private List<ExtractedLiteral> literals = new ArrayList<ExtractedLiteral>(); 
	private int state = NONE;
	private int litBegin = -1;
	private StringBuilder acc = null;
	private char pth;
	private char ith;
	
	
	public CandidateParser(String given) {
		shrunk = null;
		in = given.trim();
		acc = new StringBuilder(in.length());
	}

	public List<ExtractedLiteral> getLiterals() { return literals; }
	public String getShrunk() { return shrunk; }
	
	private static final char quote1 = '\'';
	private static final char quote2 = '"';
	private static final char escape = '\\';
	private static final char tab = '\t';
	private static final char space = ' ';
	private static final char cr = '\n';
	
	public static String shrinkAnything(String in) {
		String actual = in.trim();
		CandidateParser cp = new CandidateParser(actual);
		if (cp.onlyShrink(1, actual.length()))
			return cp.getShrunk();
		return null;
	}
	
	private boolean onlyShrink(int i, int len) {
		// ok, so we've detected the type - regular dml
		// let's see if we can find literals
		pth = in.charAt(i - 1);
		while(i < len && state != ERROR) {
			ith = in.charAt(i);
			int stateWas = state;
			switch(state) {
			case NONE:
				maybeStartLiteral(i);
				break;
			case IDENTIFIER:
				continueIdentifier(i);
				break;
			case STRINGLIT:
				maybeAccStringLit(i);
				break;
			case INTLIT:
				maybeAccIntLit(i);
				break;
			case DECLIT:
				maybeAccDecLit(i);
				break;
			case HEXLIT:
				maybeAccHexLit(i);
				break;
			case START_HEXLIT:
				maybeStartHexLit(i);
				break;
			case ERROR:
				return false;
			default:
				break;
			}
			if ((stateWas == NONE || stateWas == IDENTIFIER) && (state == NONE || state == IDENTIFIER) && !normalizeWhitespace())
				// no state change
				acc.append(ith);
			pth = ith;
			i++;
		}
		finish(i);
		if (state == ERROR) return false;
		shrunk = acc.toString();
		return true;		
	}
	
	private boolean normalizeWhitespace() {
		if (state == NONE) {
			if (ith == tab || ith == cr)
				ith = space;
			if (ith == space && pth == space)
				return true;
		}
		return false;
	}
	
	public boolean shrink() {
		Prefix type = detectType(in);
		if (type == null) return false;
		int len = in.length();
		int i = 0;
		for(; i < type.characters.length; i++)
			acc.append(in.charAt(i));
		return onlyShrink(i,len);
	}

	private void finish(int i) {
		if (state == ERROR) return;
		if (state != NONE && state != IDENTIFIER) {
			// out of characters, let's see if we can get the last literal
			if (state == STRINGLIT || state == HEXLIT) {
				// didn't see the closing quote, we're confused, give up
				state = ERROR;
				return;
			}
			if (state != INTLIT && state != DECLIT) {
				state = ERROR;
				return;
			}
			String lit = in.substring(litBegin,i);
			if (state == INTLIT)
				literals.add(ExtractedLiteral.makeIntegralLiteral(lit,acc.length()));
			else if (state == DECLIT)
				literals.add(ExtractedLiteral.makeDecimalLiteral(lit,acc.length()));
			acc.append("?");
		}		
	}
	
	private void maybeAccStringLit(int i) {
		// this chunk of code here is to capture the string '\\'
		boolean counts = false;
		if (ith == quote1 || ith == quote2) {
			counts = true;
			if (pth == escape) {
				counts = false;
				char ughth = in.charAt(i - 2);
				if (ughth == escape) {
					// counts
					counts = true;
				}
			}
		}
		if (counts) {
			String stringlit = in.substring(litBegin, i+1);
			literals.add(ExtractedLiteral.makeStringLiteral(stringlit,acc.length()));
			state = NONE;
			litBegin = -1;
			acc.append("?");
		}
	}
	
	private void maybeAccHexLit(int i) {
		// no need to worry about \\ - so if ith is quote then the hexlit is done
		if (ith == quote1) {
			String hexlit = in.substring(litBegin, i+1);
			literals.add(ExtractedLiteral.makeHexLiteral(hexlit,acc.length()));
			state = NONE;
			litBegin = -1;
			acc.append("?");
		} else if (ith == ' ') {
			// hex literals can't have spaces
			state = ERROR;
		}
	}
	
	private void maybeStartHexLit(int i) {
		if (ith == quote1) {
			state = HEXLIT;
			// don't change the litBegin
		} else {
			state = IDENTIFIER;
			// acc the pth letter and ith
			acc.append(pth);
			acc.append(ith);
			litBegin = -1;
		}
	}
	
	private void maybeAccDecLit(int i) {
		if (!Character.isDigit(ith)) {
			String declit = in.substring(litBegin, i);
			literals.add(ExtractedLiteral.makeDecimalLiteral(declit,acc.length()));
			state = NONE;
			litBegin = -1;
			acc.append("?").append(ith);
		}		
	}

	private void maybeAccIntLit(int i) {
		if ('.' == ith) {
			state = DECLIT;
		} else if (!Character.isDigit(ith)) {
			String intlit = in.substring(litBegin, i);
			literals.add(ExtractedLiteral.makeIntegralLiteral(intlit,acc.length()));
			state = NONE;
			litBegin = -1;
			acc.append("?").append(ith);
		}
	}

	private void maybeStartLiteral(int i) {
		// neither string literals nor numeric literals can start with a character
		// furthermore, disallow letter-quote-number
		/*
		
		if (Character.isLetter(ith) && !Character.isLetter(pth)) {
			if (pth == ' ' && ith == 'X' || ith == 'x') {
				state = START_HEXLIT;
				litBegin = i;
			} else {
				// we were already in the non state, try moving to the identifier state
				state = IDENTIFIER;
			}
		} else if (!Character.isLetter(pth) && pth != '_') { 
			if (quote == ith) {
				state = STRINGLIT;
				litBegin = i;
			} else if (Character.isDigit(ith)) {
				char oth = in.charAt(i - 2);
				if (Character.isLetter(oth) && quote == pth) {
					// tagged literal, ignore for now
					state = ERROR;
				} else {
					litBegin = i;
					state = INTLIT;
				}
			}
		}*/
		// restructuring the above code to this method gave a slight edge over 13m calls; since this is
		// hot path stuff I guess it's worth it
		if (!Character.isLetter(pth)) {
			if (Character.isLetter(ith)) {
				if (pth == ' ' && ith == 'X' || ith == 'x') {
					state = START_HEXLIT;
					litBegin = i;
				} else {
					state = IDENTIFIER;
				}
			} else if (pth != '_') {
				if (quote1 == ith || quote2 == ith) {
					state = STRINGLIT;
					litBegin = i;
				} else if (Character.isDigit(ith)) {
					char oth = in.charAt(i - 2);
					if (Character.isLetter(oth) && (quote1 == pth || quote2 == pth)) {
						// tagged literal, ignore for now
						state = ERROR;
					} else {
						litBegin = i;
						state = INTLIT;
					}					
				}
			}
		}
	}
	
	private void continueIdentifier(int i) {
		// we can only leave this state if we see something that isn't a character and isn't a number
		// but not for '.'
		if (ith == '.') return;
		if (!Character.isLetter(ith) && !Character.isDigit(ith)) {
			state = NONE;
		}
		return;
	}
	
	// states
	private static final int ERROR = -1;
	private static final int NONE = 0;
	private static final int STRINGLIT = 1;
	private static final int INTLIT = 2;
	private static final int DECLIT = 3;
	// some people like storing hex literals, no idea why
	private static final int HEXLIT = 4;
	// fake state - we saw ' ' 'x' - is the next character quote?
	private static final int START_HEXLIT = 5;
	// sysbench specific - we go into identifier state
	// when we see some characters, and leave it upon whitespace, or nonintegral character
	private static final int IDENTIFIER = 6;
	
	public static class Prefix {
		
		public final char[] characters;
		public final ExecutionType type;
		
		public Prefix(String v, ExecutionType et) {
			characters = v.toCharArray();
			type = et;
		}
				
	}
	
	private static final Prefix[] recognized = new Prefix[] {
		new Prefix("select",ExecutionType.SELECT),
		new Prefix("insert",ExecutionType.INSERT),
		new Prefix("delete",ExecutionType.DELETE),
		new Prefix("update",ExecutionType.UPDATE),
		new Prefix("commit",ExecutionType.TRANSACTION),
		new Prefix("begin", ExecutionType.TRANSACTION),
		new Prefix("set",   ExecutionType.SESSION)
	};
	
	public static boolean isInsert(String in) {
		Prefix p = detectType(in);
		if (p == null) return false;
		return p.type == ExecutionType.INSERT;
	}
	
	
	public static Prefix detectType(String in) {
		int len = in.length();
		if (len < 3) return null;
		int decision = -1;
		for(int i = 0; i < 6 && i < len; i++) {
			char ith = Character.toLowerCase(in.charAt(i));
			if (decision == -1) {
				for(int d = 0; d < recognized.length; d++) {
					if (ith == recognized[d].characters[0]) {
						decision = d;
						break;
					}
				}
				// not dml
				if (decision == -1) return null;
			} else {
				if (recognized[decision].characters.length <= i)
					break;
				if (ith != recognized[decision].characters[i]) {
					if (decision == 0 && ith == recognized[6].characters[i]) {
						decision = 6;
					} else {
						// doesn't match
						return null;
					}
				}
			}
		}
		return recognized[decision];
	}	
	
	public static void main(String[] ignored) {
		
		int tuples = 100000;
		StringBuffer buf = new StringBuffer();
		// specifically from sysbench
		buf.append("INSERT INTO sbtest(k, c, pad) values ");
		for(int i = 0; i < tuples; i++) {
			if (i > 0) buf.append(", ");
			buf.append("(0, ' ', 'qqqqqqqqqqwwwwwwwwwweeeeeeeeeerrrrrrrrrtttttttttt')");
		}
		try {
			CandidateParser cp = new CandidateParser(buf.toString());
			long startAt = System.nanoTime();
			cp.shrink();
			long delta = System.nanoTime() - startAt;
			System.out.println(tuples + " tuples took " + delta/1000 + " microseconds, with " + cp.getLiterals().size() + " literals");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
}
