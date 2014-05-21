// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

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
