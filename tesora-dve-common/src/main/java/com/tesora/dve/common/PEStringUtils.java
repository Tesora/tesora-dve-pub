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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public final class PEStringUtils {

	public static final String FILE_SEP = "/";
	private static final String[] EMPTY_ARRAY = new String[] {};
	private static final Pattern HEX_MATCH_REGEX = Pattern.compile("0[xX][0-9a-fA-F]+");
	
	private PEStringUtils() {
	}

	public static String buildPath(String dir, String sep, String name) {
		StringBuffer path = new StringBuffer(dir);
		if (!dir.endsWith(sep))
			path.append(sep);
		return path.append(name).toString();
	}

	public static StringBuffer toString(StringBuffer sb, String name, Collection<? extends Object> values) {
		sb.append(name).append('{');
		toArrayString(sb, values);
		return sb.append('}');
	}

	public static StringBuffer toArrayString(StringBuffer sb, Collection<? extends Object> values) {
		int i = 0;
		if (values != null) {
			for (Object o : values) {
				if (i++ > 0)
					sb.append(',');
				if (o == null)
					sb.append("null");
				else
					sb.append(o.toString());
			}
		} else
			sb.append("null");
		return sb;
	}

	public static String toString(String name, Collection<? extends Object> values) {
		return toString(new StringBuffer(), name, values).toString();
	}

	public static <T> String toString(Class<T> theClass, Collection<? extends Object> values) {
		return toString(new StringBuffer(), theClass.getSimpleName(), values).toString();
	}

	public static <K, V> StringBuffer toString(StringBuffer sb, String name, Map<K, V> values) {
		sb.append(name).append('{');
		int i = 0;
		if (values != null) {
			for (Entry<K, V> e : values.entrySet()) {
				if (i++ > 0)
					sb.append(',');
				sb.append(e.getKey().toString()).append('/').append(e.getValue().toString());
			}
		} else
			sb.append("null");
		return sb.append('}');
	}

	public static <K, V> String toString(String name, Map<K, V> values) {
		return toString(new StringBuffer(), name, values).toString();
	}

	public static String[] toArray(Collection<String> values) {
		return values.toArray(EMPTY_ARRAY);
	}

	/*
	 * Returns a prefix needing to be pre-pended to a key for calling
	 * Properties.getProperty. Handles null prefix; will add the trailing "." if
	 * not already present.
	 */
	public static String normalizePrefix(String prefix) {
		String newPrefix = StringUtils.defaultString(prefix);
		if (newPrefix.length() > 0 && !StringUtils.endsWith(newPrefix, ".")) {
			newPrefix = newPrefix + ".";
		}
		return newPrefix;
	}
	
	/**
	 * Get the stack trace as a string value
	 * 
	 * @param e
	 * @return
	 */
	public static String toString(Throwable e)
	{
		StringWriter stringWriter = new StringWriter();
		e.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}
	
	/**
	 * This method is used to convert a string pattern match (e.g. a SQL LIKE
	 * expression) into a Java Pattern that can be used in a Matcher
	 * 
	 * @param likeExpr - a string expression to be converted to a Pattern
	 * @return - Pattern - a Pattern object representing the likeExpr
	 */
	public static Pattern buildSQLPattern(String likeExpr) {
		// let's build a regex out of a like string...
		char[] sqlPattern = likeExpr.trim().toLowerCase().toCharArray();
		StringBuffer javaPattern = new StringBuffer();
		boolean escape = false;
		for(int i = 0; i < sqlPattern.length; i++) {
			if (escape) {
				javaPattern.append(sqlPattern[i]);
				escape = false;
			} else if (sqlPattern[i] == '\\')
				escape = true;
			else if (".?+*|[]{}()^$-".indexOf(sqlPattern[i]) > -1) {
				javaPattern.append("\\");
				javaPattern.append(sqlPattern[i]);
			} else if (sqlPattern[i] == '%') 
				javaPattern.append(".*");
			else if (sqlPattern[i] == '_')
				javaPattern.append(".");
			else 
				javaPattern.append(sqlPattern[i]);
		}
		
		return Pattern.compile(javaPattern.toString());
	}

	public static String dequote(String in) {
		if (in.length() == 0) return in;
		if (in.charAt(0) == '\'' && in.endsWith("'") ||
				in.charAt(0) == '\"' && in.endsWith("\"") ||
				in.charAt(0) == '`' && in.endsWith("`")) {
			// strip off the quotes
			return in.substring(1, in.length() - 1);
		}
		return in;
	}

	public static String escapeSingleQuoteIfNecessary(String t) {
		// if enclosed with double quotes and contains a single quote we need to escape the single quotes
		// because we ultimately enclose the entire literal with the ' character (ie. replace " with ')
		if (t.contains("'")) {
			// unfortunately cannot just replace ' if \' we have to leave it alone
			int searchStartIndex = 0;
			int quoteIndex = t.indexOf("'", searchStartIndex);
			int escapedQuoteIndex = t.indexOf("\\'", searchStartIndex);
			while (quoteIndex != -1) {
				if ((quoteIndex - escapedQuoteIndex) != 1) {
					t = t.substring(0, quoteIndex) + "\\" + t.substring(quoteIndex);
					quoteIndex += 1;
				}
				searchStartIndex = quoteIndex + 1;
				quoteIndex = t.indexOf("'", searchStartIndex);
				escapedQuoteIndex = t.indexOf("\\'", searchStartIndex);
			}
		}
		
		return t;
	}

	/**
	 * Returns true if a value is given and has any normal semblance of being positive
	 * 
	 * for example: "true", "on", "23", "yes" return true
	 *              "false", null, "0", "garbage" return false
	 *              
	 * @param value
	 * @return
	 */
	public static boolean toBoolean(String value) {
		try {
			if (value != null && value.length() > 0
					&& (value.equalsIgnoreCase("true") || value.equalsIgnoreCase(PEConstants.YES)
							|| value.equalsIgnoreCase("on") || Integer.parseInt(value) > 0))
				return true;
		} catch (NumberFormatException nfe) {
			// ignore
		}

		return false;
	}

	public static boolean isHexNumber(final String value) {
		return HEX_MATCH_REGEX.matcher(value).matches();
	}

}
