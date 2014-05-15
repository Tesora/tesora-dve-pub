// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

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
