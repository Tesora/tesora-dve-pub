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

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public class AnalyzerPlanningError extends AnalyzerResult {

	private final Throwable fault;

	public AnalyzerPlanningError(final SchemaContext sc, final String sql, final SourcePosition pos, final DMLStatement s, final Throwable fault) {
		super(sc, sql, pos, s);
		this.fault = fault;
	}

	public Throwable getFault() {
		return this.fault;
	}

}
