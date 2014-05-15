// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

import java.io.PrintStream;

import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;

public class SourcePosition {

	protected final LineInfo info;

	public SourcePosition(LineInfo li) {
		info = li;
	}

	public LineInfo getLineInfo() {
		return info;
	}

	public long getPosition() {
		if (info.getLineNumber() == 0) {
			return info.getConnectionID();
		}

		return info.getLineNumber();
	}

	public void describe(PrintStream ps) {
		ps.println("-- Line " + getPosition() + " -------------------");
	}

}
