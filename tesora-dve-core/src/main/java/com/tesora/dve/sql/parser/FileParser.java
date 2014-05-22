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


import io.netty.util.CharsetUtil;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.sql.parser.ParserInvoker.TaggedLineInfo;
import com.tesora.dve.sql.parser.filter.LogFileFilter;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.DDLStatement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;

public class FileParser {

	static final String lf = "\n";//System.getProperty("line.separator");

	public void parseOneSqlFile(String fileName, ParserInvoker invoker) throws Throwable {
		parseOneFileNative(PEFileUtils.getResourceStream(getClass(), fileName), new LogLineBuffer(), invoker);
	}

	public void parseOneMysqlLogFile(String fileName, ParserInvoker invoker) throws Throwable {
		parseOneFileNative(PEFileUtils.getResourceStream(getClass(), fileName), new MysqlLogLineBuffer(), invoker);
	}

	public void parseOneMysqlLogFile(Class<?> relativeTo, String fileName, ParserInvoker invoker) throws Throwable {
		parseOneFileNative(PEFileUtils.getResourceStream(relativeTo, fileName), new MysqlLogLineBuffer(), invoker);
	}
	
	public void parseOneFilePELog(Class<?> relativeTo, String fileName, ParserInvoker invoker) throws Throwable {
		parseOneFilePELog(PEFileUtils.getResourceStream(relativeTo, fileName), invoker);
	}
	
	public void parseOneMysqlLogFile(InputStream in, ParserInvoker invoker, Charset encoding) throws Throwable {
		parseOneMysqlLogFile(in, invoker, encoding, null);
	}
	
	public void parseOneMysqlLogFile(InputStream in, ParserInvoker invoker, Charset encoding, LogFileFilter filter) throws Throwable {
		parseOneFileNative(in, new MysqlLogLineBuffer(filter), invoker, encoding);
	}

	public void parseOnePELogFile(InputStream in, ParserInvoker invoker, Charset encoding) throws Throwable {
		parseOneFileNative(in, new PELogLineBuffer(), invoker, encoding);
	}
	
	public void parseOneFileNative(InputStream in, LogLineBuffer buffer, ParserInvoker invoker) throws Throwable {
		parseOneFileNative(in, buffer, invoker, CharsetUtil.ISO_8859_1);
	}
	
	public void parseOneFileNative(InputStream in, LogLineBuffer buffer, ParserInvoker invoker, Charset encoding) throws Throwable {
		InputStreamReader isr = new InputStreamReader(in,encoding);
		char[] lsep = lf.toCharArray();
		int lsp = 0;
		long counter = 0;
		char[] readBuf = new char[1024];
		int read = 0;
		// carriage return stuff requires a first carriage return now
		buffer.add('\n',true);
		do {
			read = isr.read(readBuf, 0, 1024);
			// check for newlines in the buffer
			for(int i = 0; i < read; i++) {
				char c = readBuf[i];
				if (c == lsep[lsp]) {
					lsp = lsp + 1;
				} else {
					lsp = 0;
				}
				if (lsp == lsep.length) {
					lsp = 0;
					buffer.add(c,true);
					boolean haveStatement = buffer.haveStatement();
					if (haveStatement) {
						// might be ignored
						TaggedStatement ts = buffer.getStatement();
						if (ts == null)
							continue;
						invoker.parseOneLineInternal(new LineInfo(counter++, invoker.getOptions(), ts.getConnectionID()), ts.getStatement());
					}
				} else {
					buffer.add(c,false);
				}
			}
		} while (read > -1);
		if (buffer.haveStatement()) {
			TaggedStatement stmt = buffer.getStatement();
			if (stmt != null)
				invoker.parseOneLineInternal(new LineInfo(counter++, invoker.getOptions(), stmt.getConnectionID()), stmt.getStatement());
		}
		buffer.close();
		isr.close();
	}

	public void parseOneFilePELog(InputStream in, ParserInvoker invoker) throws Throwable {
		// parse what the Converting parser invoker creates
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		StringBuffer command = null;
		TaggedLineInfo current = null;
		boolean done = false;
		do {
			String line = br.readLine();
			if (line == null)
				done = true;
			else {
				if (line.startsWith("#pecomment")) {
					// ignore
				} else if (line.startsWith("#pe")) {
					String parts[] = line.split(" ");
					if (parts.length > 2) {
						if (command != null) {
							throw new Exception("Badly formatted PELog");
						} 
						command = new StringBuffer();
						current = TaggedLineInfo.fromFileFormat(line);
					} else {
						if (command == null) {
							throw new Exception("Badly formatted PELog");
						}
						String stmt = command.toString();
						invoker.parseOneLineInternal(current, stmt);
						command = null;
						current = null;
					}
				} else if (command == null) {
					throw new Exception("Badly formatted PELog");
				} else {
					command.append(line).append(lf);
				}
			}
		} while(!done);
		if (command != null)
			throw new Exception("Badly terminated PELog");
		br.close();
	}
	
	// the output block looks like
	// # pe n [SUDT] other-options
	// sql statement here
	// # pe end
	// where n is the statement number, and the second flag is
	// _S_elect
	// _U_pdate
	// _D_dl
	// _T_ransient (session, txn, etc.)
	public static void convertNativeLog(InputStream in, OutputStream out, boolean force, LogFileFilter filter) throws Throwable {
		ConvertingParserInvoker cpi = new ConvertingParserInvoker(out,force);
		new FileParser().parseOneMysqlLogFile(in, cpi, CharsetUtil.ISO_8859_1, filter);
		cpi.close();
	}
	
	public static void convertPELog(InputStream in, OutputStream out, boolean force) throws Throwable {
		ConvertingParserInvoker cpi = new ConvertingParserInvoker(out, force);
		new FileParser().parseOnePELogFile(in, cpi, CharsetUtil.ISO_8859_1);
		cpi.close();
	}
		
	public static class ConvertingParserInvoker extends ParserInvoker {

		private PrintWriter out;
		private boolean forceContinue;
		
		public ConvertingParserInvoker(OutputStream outstream, boolean force) {
			super(ParserOptions.TEST.setFailEarly());
			out = new PrintWriter(new OutputStreamWriter(outstream, CharsetUtil.ISO_8859_1), true);
			forceContinue = true;
		}
	
		public void close() {
			out.close();
		}
		
		@Override
		public String parseOneLine(LineInfo info, String line) throws Exception {
			LineTag tag = null;
			String updateTable = null;
			// we could get Connect and Quit statements too - look for those early
			String trimmed = line.trim();
			if (trimmed.equals("Quit")) {
				tag = LineTag.DISCONNECT;
			} else if (trimmed.equals("Connect")) {
				tag = LineTag.CONNECT;
			} else {
				List<Statement> stmts = null;
				try {
					stmts = InvokeParser.parse(InvokeParser.buildInputState(trimmed,null), ParserOptions.TEST.setFailEarly(), null).getStatements();
				} catch (ParserException pe) {
					if (forceContinue) {
						// if we get an exception, just write out a session statement
						tag = LineTag.SESSION;
						System.err.println("Unable to parse line " + info.getLineNumber() + ", statement is:");
						System.err.println(trimmed);
						pe.printStackTrace();
						stmts = Collections.emptyList();
					} else
						throw pe;
				}
				if (stmts.size() > 1) 
					throw new Exception("What do I do with an input line that results in more than one statement? '" + line + "'");
				if (!stmts.isEmpty()) {
					Statement s = stmts.get(0);
					if (s instanceof ProjectingStatement) {
						ProjectingStatement ps = (ProjectingStatement) s;
						if (ps.getOrderBysEdge().has()) {
							tag = LineTag.SELECT_ORDERED;
						} else {
							tag = LineTag.SELECT;
						}
					} else if (s instanceof DMLStatement) {
						tag = LineTag.UPDATE;
						if (s instanceof InsertIntoValuesStatement) {
							InsertIntoValuesStatement is = (InsertIntoValuesStatement) s;
							updateTable = is.getTableInstance().getSpecifiedAs(null).get();
						} else if (s instanceof UpdateStatement) {
							UpdateStatement us = (UpdateStatement) s;
							updateTable = us.getTables().get(0).getBaseTable().getSpecifiedAs(null).get();							
						} else if (s instanceof DeleteStatement) {
							DeleteStatement ds = (DeleteStatement) s;
							updateTable = ds.getTables().get(0).getBaseTable().getSpecifiedAs(null).get();
						} else if (s instanceof InsertIntoSelectStatement) {
							InsertIntoSelectStatement iiss = (InsertIntoSelectStatement) s;
							updateTable = iiss.getTableInstance().getSpecifiedAs(null).get();
						}
					} else if (s instanceof DDLStatement) {
						tag = LineTag.DDL;
					} else {
						tag = LineTag.SESSION;
					}
				}
			}
			TaggedLineInfo tli = new TaggedLineInfo(info.getLineNumber(), null, info.getConnectionID(), tag);
			if (updateTable != null)
				tli.setOption(new LineInfoOption<String>(TaggedLineInfo.TABLE_KEY,updateTable));
			out.println(tli.toFileFormat());
			out.println(line);
			out.println("#pe end");
			return line;
		}
	}
	
}
