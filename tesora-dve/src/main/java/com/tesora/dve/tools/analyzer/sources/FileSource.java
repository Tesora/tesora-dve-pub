// OS_STATUS: public
package com.tesora.dve.tools.analyzer.sources;

import io.netty.util.CharsetUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.parser.FileParser;
import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.tools.analyzer.Analyzer;
import com.tesora.dve.tools.analyzer.AnalyzerSource;
import com.tesora.dve.tools.analyzer.SourcePosition;

public class FileSource extends AnalyzerSource {

	private String type;
	private InputStream logFile;
	private final String descr;

	@SuppressWarnings("resource")
	public FileSource(String fileType, File file) throws PEException {
		try {
			init(fileType, new FileInputStream(file));
			this.descr = "FileSource using Type:" + type + " File:" + file.getAbsolutePath();
		} catch (final IOException e) {
			throw new PEException("Unable to open file " + file.getAbsolutePath(), e);
		}
	}

	public FileSource(String fileType, InputStream is) throws PEException {
		init(fileType, is);

		this.descr = "FileSource using InputStream";
	}

	@Override
	public String getDescription() {
		return descr;
	}

	private void init(String fileType, InputStream is) throws PEException {
		if (is == null) {
			throw new PEException("Must specify the analysis input file");
		}

		if (fileType == null) {
			throw new PEException("Must specify file type");
		} else if (!fileType.equals("mysql") && !fileType.equals("plain")) {
			throw new PEException("Unknown file type: " + fileType);
		}

		this.type = fileType;
		this.logFile = is;
	}

	@Override
	public void analyze(Analyzer sink) throws Throwable {
		sink.setSource(this);
		if (type.equals("mysql")) {
			mysqlLogFile(logFile, sink.getInvoker());
		} else if (type.equals("plain")) {
			plainLogFile(logFile, sink.getInvoker());
		}
		sink.onFinished();
		sink.setSource(null);
	}

	@Override
	public void closeSource() throws IOException {
		logFile.close();
	}

	private static void mysqlLogFile(InputStream is, ParserInvoker invoker) throws Throwable {
		new FileParser().parseOneMysqlLogFile(is, invoker, CharsetUtil.ISO_8859_1);
	}

	private static void plainLogFile(InputStream is, ParserInvoker invoker) throws Throwable {
		final BufferedReader buf = new BufferedReader(new InputStreamReader(is));
		String line = null;
		int lineno = 0;
		do {
			line = buf.readLine();
			if (line == null) {
				continue;
			}
			lineno++;
			if (line.charAt(0) == '#') {
				continue;
			}
			invoker.parseOneLine(new LineInfo(lineno, null, 0), line);
		} while (line != null);
	}

	@Override
	public SourcePosition convert(LineInfo li) {
		return new SourcePosition(li);
	}

}
