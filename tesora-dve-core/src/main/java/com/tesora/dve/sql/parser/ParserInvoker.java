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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ParserInvoker {
	
	private ParserOptions opts;
	
	public ParserInvoker(ParserOptions opts) {
		this.opts = opts;
	}
	
	protected ParserOptions getOptions() {
		return this.opts;
	}
	
	public abstract String parseOneLine(LineInfo info, String line) throws Throwable;
	
	protected String parseOneLineInternal(LineInfo info, String line) throws Throwable {
//		TestParser.echo("In@" + info + ": '" + line + "'");
		String out = parseOneLine(info, line);
//		TestParser.echo("Out: '" + out + "'");
		return out;
	}
	
	public static class LineInfoOption<T> {
		
		protected T target;
		protected String name;
		
		public LineInfoOption(String n, T v) {
			target = v;
			name = n;
		}
		
		public String getName() {
			return name;
		}
		
		public T getValue() {
			return target;
		}
		
		public void setValue(T v) {
			target = v;
		}
	}
	
	public static class LineInfo {
		
		protected Map<String, LineInfoOption<?>> values;
		
		protected static final String LINE_NUMBER_KEY = "line";
		protected static final String CONN_ID_KEY = "conn";
		
		private ParserOptions options;
		
		public LineInfo(List<LineInfoOption<?>> opts) {
			values = new HashMap<String, LineInfoOption<?>>();
			for(LineInfoOption<?> lio : opts) 
				values.put(lio.getName(),lio);
			options = null;
		}
		
		public LineInfo(long lineno, ParserOptions opts, int connID) {
			values = new HashMap<String, LineInfoOption<?>>();
			addLongOption(LINE_NUMBER_KEY, lineno);
			addIntOption(CONN_ID_KEY,connID);
			options = opts;
		}
		
		public LineInfo(LineInfo e, ParserOptions newopt) {
			values = new HashMap<String, LineInfoOption<?>>(e.values);
			options = newopt;
		}
		
		public void addIntOption(String name, int v) {
			values.put(name, new LineInfoOption<Integer>(name, v));
		}
		
		public Integer getIntOption(String name) {
			@SuppressWarnings("unchecked")
			LineInfoOption<Integer> any = (LineInfoOption<Integer>) values.get(name);
			if (any == null) return null;
			return any.getValue();
		}
		
		public void addStringOption(String name, String v) {
			values.put(name, new LineInfoOption<String>(name,v));
		}
		
		public String getStringOption(String name) {
			@SuppressWarnings("unchecked")
			LineInfoOption<String> any = (LineInfoOption<String>) values.get(name);
			if (any == null) return null;
			return any.getValue();
		}
		
		public void addLongOption(String name, long v) {
			values.put(name, new LineInfoOption<Long>(name, v));
		}
		
		public LineInfoOption<?> getOption(String name) {
			return values.get(name);
		}
		
		public void setOption(LineInfoOption<?> opt) {
			values.put(opt.getName(), opt);
		}
		
		public long getLineNumber() {
			@SuppressWarnings("unchecked")
			LineInfoOption<Long> opt = (LineInfoOption<Long>) getOption(LINE_NUMBER_KEY);
			if (opt == null) return -1;
			return opt.getValue();
		}

		public void setLineNumber(long v) {
			@SuppressWarnings("unchecked")
			LineInfoOption<Long> opt = (LineInfoOption<Long>) getOption(LINE_NUMBER_KEY);
			opt.setValue(new Long(v));
		}
		
		public int getConnectionID() {
			@SuppressWarnings("unchecked")
			LineInfoOption<Integer> opt = (LineInfoOption<Integer>) getOption(CONN_ID_KEY);
			if (opt == null) return -1;
			return opt.getValue();
		}
		
		public ParserOptions getOptions() {
			return options;
		}
		
		public LineInfo setNoisy(ParserOptions opts) {
			return new LineInfo(this, opts);
		}
		
		@Override
		public String toString() {
			long lineno = getLineNumber();
			int connID = getConnectionID();
			return "stmt: " + lineno + (connID == -1 ? "" : " on conn " + connID);
		}
	}
	
	public enum LineTag {
		
		SELECT,
		SELECT_ORDERED,
		DISCONNECT,
		CONNECT,
		UPDATE,
		DDL,
		SESSION;
		
		public static LineTag fromFile(String in) {
			for(LineTag lt : LineTag.values()) {
				if (lt.name().equals(in)) {
					return lt;
				}
			}
			throw new RuntimeException("No such line tag: '" + in + "'");
		}
	}
	
	public static class TaggedLineInfo extends LineInfo {
		
		protected static final String TAG_KEY = "tag";
		protected static final String UPDATE_KEY = "update";
		protected static final String TABLE_KEY = "table";
		
		public TaggedLineInfo(List<LineInfoOption<?>> vals) {
			super(vals);
		}
		
		public TaggedLineInfo(long lineno, ParserOptions opts, int connID, LineTag tag) {
			this(lineno, opts, connID, tag, null);
		}
		
		@SuppressWarnings("rawtypes")
		public TaggedLineInfo(long lineno, ParserOptions opts, int connID, LineTag tag, LineInfoOption[] extras) {
			super(lineno, opts, connID);
			setOption(new LineInfoOption<LineTag>(TAG_KEY, tag));
		}

		public TaggedLineInfo(TaggedLineInfo tli, ParserOptions newOpts) {
			super(tli, newOpts);
		}
		
		@SuppressWarnings("unchecked")
		public LineTag getTag() {
			return ((LineInfoOption<LineTag>)getOption(TAG_KEY)).getValue();
		}
		
		@SuppressWarnings("unchecked")
		public String getTable() {
			LineInfoOption<String> lio = (LineInfoOption<String>) getOption(TABLE_KEY);
			if (lio == null) return null;
			return lio.getValue();
 		}
		
		@Override
		public LineInfo setNoisy(ParserOptions opts) {
			return new TaggedLineInfo(this, opts);
		}
		
		@Override
		public String toString() {
			return super.toString() + " tag: " + getTag();
		}
		
		public String toFileFormat() {
			StringBuffer buf = new StringBuffer();
			buf.append("#pe");
			for(LineInfoOption<?> lio : values.values()) {
				String key = lio.getName();
				Object value = lio.getValue();
				buf.append(" ").append(key).append("=").append(value);
			}
			return buf.toString();
		}
		
		public static TaggedLineInfo fromFileFormat(String in) {
			String[] parts = in.split(" ");
			if ("#pe".equals(parts[0])) {
				ArrayList<LineInfoOption<?>> opts = new ArrayList<LineInfoOption<?>>();
				for(int i = 1; i < parts.length; i++) {
					String[] kv = parts[i].split("=");
					String key = kv[0];
					String value = kv[1];
					if (TAG_KEY.equals(key)) {
						opts.add(new LineInfoOption<LineTag>(TAG_KEY,LineTag.fromFile(value)));
					} else if (LINE_NUMBER_KEY.equals(key)) {
						opts.add(new LineInfoOption<Long>(LINE_NUMBER_KEY,Long.parseLong(value)));
					} else if (CONN_ID_KEY.equals(key)) {
						opts.add(new LineInfoOption<Integer>(CONN_ID_KEY,Integer.parseInt(value)));
					} else {
						opts.add(new LineInfoOption<String>(key,value));
					}
				}
				return new TaggedLineInfo(opts);
			} else {
				throw new RuntimeException("Unknown token '" + in + "'");
			}
		}
		
	}
	
}