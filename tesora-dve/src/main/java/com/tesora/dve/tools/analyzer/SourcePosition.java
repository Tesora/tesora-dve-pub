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
