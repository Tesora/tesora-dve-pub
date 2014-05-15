// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

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
