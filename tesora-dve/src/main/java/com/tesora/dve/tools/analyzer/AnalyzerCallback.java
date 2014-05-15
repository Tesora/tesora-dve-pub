// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

import java.io.PrintStream;

public abstract class AnalyzerCallback {

	private PrintStream output;

	public AnalyzerCallback() {
		this(System.out);
	}

	public AnalyzerCallback(final PrintStream ps) {
		this.output = ps;
	}

	public void setOutputStream(final PrintStream ps) {
		if (ps == null) {
			throw new IllegalArgumentException();
		}
		this.output = ps;
	}

	public PrintStream getOutputStream() {
		return this.output;
	}

	public void close() {
		this.output.close();
	}

	public abstract void onResult(final AnalyzerResult bar);

}
