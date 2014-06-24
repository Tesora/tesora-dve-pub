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
