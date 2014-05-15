// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public class AnalyzerPlanningNotice extends AnalyzerResult {

	private final String message;

	protected AnalyzerPlanningNotice(final SchemaContext sc, final String sql, final SourcePosition pos, final DMLStatement s, final String message) {
		super(sc, sql, pos, s);
		this.message = message;
	}

	public String getMessage() {
		return this.message;
	}

}
