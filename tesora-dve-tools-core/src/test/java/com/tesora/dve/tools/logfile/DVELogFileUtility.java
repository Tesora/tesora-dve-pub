package com.tesora.dve.tools.logfile;

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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.parser.FileParser;
import com.tesora.dve.sql.parser.FileParser.ConvertingParserInvoker;
import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.parser.filter.IncludedConnectionIdFilter;
import com.tesora.dve.sql.parser.filter.LogFileFilter;
import com.tesora.dve.sql.schema.Capability;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.standalone.PETest;

// just aggregates some utilities around pe log files
public class DVELogFileUtility {

	// we have two major modes - converting mode and everything else
	private static final String convertMode = "convert";
	private static final String utilMode = "util";
	
	// flags for converting
	private static final String forceContinueFlag = "force";
	private static final String dirFlag = "dir";
	private static final String inFlag = "in";
	private static final String outFlag = "out";
	private static final String peFlag = "pe";
	private static final String connIdFilterInFlag = "connIdIn";
	
	// flags for  general utility stuff
	private static final String concatFlag = "concat";
	
	private static final Parameter params[] = new Parameter[] {
		new Switch(convertMode,'c',convertMode,"Converting mode - params indicate mysql log files to convert"),
		new Switch(utilMode,'u',utilMode,"Utility mode - params indicate pelogfiles to manipulate"),
		new Switch(dirFlag,'d',dirFlag,"Treat in/out as directories"),
		new Switch(concatFlag,'a',concatFlag,"Concatenate input pe log files into an output pe log file"),
		new Switch(forceContinueFlag,'f',forceContinueFlag,"Continue upon parse error (emits failing lines as session statements)"),
		new Switch(peFlag,'p',peFlag,"Convert src files as pe log files"),
		new FlaggedOption(inFlag,JSAP.STRING_PARSER,null,true,'i',inFlag,"Input file/directory (maybe repeated)").setAllowMultipleDeclarations(true),
		new FlaggedOption(outFlag,JSAP.STRING_PARSER,null,true,'o',outFlag,"Output file/directory (file only for util mode)"),
		new FlaggedOption(connIdFilterInFlag,JSAP.STRING_PARSER,null,false,'n',connIdFilterInFlag,"Input file or list of connection IDs to include").setAllowMultipleDeclarations(true),
	};

	private static void convert(boolean pe, String left, String right, boolean dir, boolean force, LogFileFilter filter) throws Throwable {
		if (dir) {
			File srcdir = new File(left);
			File targdir = new File(right);
			File[] chilluns = srcdir.listFiles();
			for(File f : chilluns) {
				if (f.getName().endsWith(".log")) {
					String relativeName = f.getName().substring(0, f.getName().length() - 4);
					File outName = new File(targdir,relativeName + ".pelog");
					convertOneFile(pe, f.getAbsolutePath(),outName.getAbsolutePath(),force,filter);
				}
			}
		} else {
			convertOneFile(pe, left,right,force,filter);
		}		
	}
	
	
	public static void main(String[] args) {
		String[] left = null;
		String right = null;
		boolean dir = false;
		boolean force = false;
		Boolean convert = null;
		boolean concat = false;
		boolean aspe = false;
		String[] connIdsIn = null;
		try {
			SimpleJSAP jsap = new SimpleJSAP("Mysql log file converter",
					"Convert mysql log files to pe log files", params);
			JSAPResult options = jsap.parse(args);
			if (jsap.messagePrinted())
				return;
			dir = options.getBoolean(dirFlag);
			force = options.getBoolean(forceContinueFlag);
			left = options.getStringArray(inFlag);
			right = options.getString(outFlag);
			concat = options.getBoolean(concatFlag);
			aspe = options.getBoolean(peFlag);
			connIdsIn = options.getStringArray(connIdFilterInFlag);
			if (!options.getBoolean(utilMode) && !options.getBoolean(convertMode)) {
				System.err.println("Must specify one of -u or -c");
				return;
			} else if (options.getBoolean(utilMode)) {
				convert = Boolean.FALSE;
			} else if (options.getBoolean(convertMode)) {
				convert = Boolean.TRUE;
			}
		} catch (JSAPException e) {
			e.printStackTrace();
			return;
		}
		
		LogFileFilter filter = null;
		
		if (connIdsIn != null) {
			filter = buildIncludedConnectionIdFilter(connIdsIn);
		}
		
		try {
			TestHost.startServicesTransient(PETest.class);
			if (Boolean.TRUE.equals(convert)) {
				if (left.length > 1) {
					System.err.println("Too many paramters to convert input (-i)");
					return;
				}
				convert(aspe,left[0],right,dir,force,filter);
			} else {
				if (concat) {
					// eventually we'll have more commands here, I guess
					concatenate(left, right, dir);
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			TestHost.stopServices();
		}
		
	}

	private static void convertOneFile(boolean pe, String orig, String xlated, boolean force, LogFileFilter filter) throws Throwable {
		System.out.println("Converting " + orig + " to " + xlated);
		try (final FileInputStream fis = new FileInputStream(orig);
				final FileOutputStream fos = new FileOutputStream(xlated)) {
			if (pe) {
				FileParser.convertPELog(fis, fos, force);
			} else {
				final NativeTypeCatalog types = Singletons.require(HostService.class).getDBNative().getTypeCatalog();
				ConvertingParserInvoker withContext = new ConvertingParserInvoker(fos,force) {
					@Override
					public SchemaContext buildContext() {
						// more reasons to hate self
						LogFileContext lfc = new LogFileContext(types);
						return SchemaContext.createContext(lfc,lfc,types,Capability.PARSING_ONLY);
					}
				};
				FileParser.convertNativeLog(fis, filter, withContext);
			}
		}
	}

	// ugh, this is a pain in the arse
	private static void concatenate(String[] left, String right, boolean dir) throws Throwable {
		// so, what we're going to do is just read in the lhs, and write them out to the rhs, adjusting line numbers as we go
		// new FileParser().parseOneFilePELog(InputStream in, ParserInvoker invoker) throws Throwable {
		FileOutputStream fos = new FileOutputStream(right);
		PrintWriter out = new PrintWriter(new OutputStreamWriter(fos, CharsetUtil.ISO_8859_1), true);
		long offset = 0L;
		ArrayList<File> infiles = new ArrayList<File>();
		for(String l : left) {
			if (dir) {
				File srcdir = new File(l);
				File[] chilluns = srcdir.listFiles();
				for(File f : chilluns) {
					if (f.getName().endsWith(".pelog")) 
						infiles.add(f);
				}
			} else {
				infiles.add(new File(l));
			}
		}
		for(File f : infiles) {
			System.out.println("Concatenating " + f.getAbsolutePath() + " onto " + right);
			try (final FileInputStream fis = new FileInputStream(f)) {
				AdjustingParserInvoker api = new AdjustingParserInvoker(out, offset);
				new FileParser().parseOneFilePELog(fis, api);
				offset = api.getLastLine() + 1; // + 1 so the 0 line next time around is correct
			}
		}
		out.close();
		fos.close();
	}

	private static IncludedConnectionIdFilter buildIncludedConnectionIdFilter(
			String[] cmdLineArgs) {
		IncludedConnectionIdFilter ret = null;

		if (cmdLineArgs == null) {
			return ret;
		}

		Set<Integer> connIds = new HashSet<Integer>();

		for (String connId : cmdLineArgs) {
			// check if it's a file
			try {
				File outName = new File(connId);
				if (outName.exists()) {
					// a file so open and read each line
					BufferedReader br = new BufferedReader(
							new InputStreamReader(new FileInputStream(outName)));
					try {
						String s = br.readLine().trim();
						while (s != null) {
							try {
								Integer i = Integer.parseInt(s);
								connIds.add(i);
								s = br.readLine().trim();
							} catch (Exception e) {
								// do nothing
								s = br.readLine().trim();
							}
						}
					} finally {
						br.close();
					}
				} else {
					// not a file so check if it's a number
					try {
						Integer i = Integer.parseInt(connId);
						connIds.add(i);
					} catch (Exception e) {
						// do nothing
					}
				}
			} catch (Exception e) {
				// do nothing
			}
		}

		if (connIds.size() > 0) {
			ret = new IncludedConnectionIdFilter(connIds);
		}

		return ret;
	}
	
	private static class AdjustingParserInvoker extends ParserInvoker {

		private PrintWriter out;
		private long lineOffset;
		private long lastLine;

		public AdjustingParserInvoker(PrintWriter target, long offset) {
			super(null);
			out = target;
			lineOffset = offset;
			lastLine = -1;
		}
		
		public long getLastLine() {
			return lastLine;
		}
		
		@Override
		public String parseOneLine(LineInfo info, String line)
				throws Throwable {
			TaggedLineInfo tli = (TaggedLineInfo) info;
			long lineno = info.getLineNumber();
			lastLine = lineno + lineOffset;
			tli.setLineNumber(lastLine);
			out.println(tli.toFileFormat());
			out.println(line);
			out.println("#pe end");
			return line;

		}
		
	}
	
}
