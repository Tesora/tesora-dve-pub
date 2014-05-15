// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

import org.apache.log4j.Logger;

import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;

public abstract class AnalyzerSource implements AutoCloseable {

	public abstract void analyze(Analyzer a) throws Throwable;

	public abstract SourcePosition convert(LineInfo li);

	public abstract String getDescription();

	public abstract void closeSource() throws Exception;

	@Override
	public final void close() {
		try {
			closeSource();
		} catch (final Exception e) {
			Logger.getLogger(this.getClass()).warn("Failed to close '" + this.getDescription() + "'", e);
		}
	}
}
