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
import java.util.ArrayList;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;

public class AnalyzerPlanningResult extends AnalyzerResult {

	private final ExecutionPlan plan;

	public AnalyzerPlanningResult(final SchemaContext sc, final String sql, final SourcePosition pos, final DMLStatement s, final ExecutionPlan plan) {
		super(sc, sql, pos, s);
		this.plan = plan;
	}

	public ExecutionPlan getPlan() {
		return this.plan;
	}

	public int getRedistributionStepCount() {
		return this.plan.getSequence().getRedistributionStepCount(this.getSchemaContext());
	}

	public void printPlan(final PrintStream ps) {
		ps.println("Execution Plan:");
		final ArrayList<String> buf = new ArrayList<String>();
		this.plan.getSequence().display(this.getSchemaContext(), buf, "  ", null);
		for (final String s : buf) {
			ps.println(s);
		}
	}

}
