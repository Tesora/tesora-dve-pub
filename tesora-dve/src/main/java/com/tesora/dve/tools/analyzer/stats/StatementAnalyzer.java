package com.tesora.dve.tools.analyzer.stats;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.tools.analyzer.Analyzer;
import com.tesora.dve.tools.analyzer.AnalyzerOptions;
import com.tesora.dve.tools.analyzer.SourcePosition;
import com.tesora.dve.tools.analyzer.sources.FrequenciesSource;

public class StatementAnalyzer extends Analyzer {

	private static final Logger logger = Logger.getLogger(StatementAnalyzer.class);

	private final StatsVisitor visitor;

	public StatementAnalyzer(AnalyzerOptions opts, StatsVisitor sv) throws Exception {
		super(opts);
		this.visitor = sv;
	}

	@Override
	public void onFinished() throws PEException {
		// noop
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onStatement(String sql, SourcePosition sp, Statement s)
			throws Throwable {
		if (s instanceof DMLStatement) {
			final Integer freqObj = sp.getLineInfo().getIntOption(FrequenciesSource.FREQUENCY_TAG);
			final int freq = (freqObj == null ? 0 : freqObj.intValue());
			final DMLStatement dmls = (DMLStatement) s;
			final StatementAnalysis<?> stmtStats = build(tee.getPersistenceContext(), sql, freq, dmls);
			visitor.beginStmt(stmtStats);
			visitStatement(tee.getPersistenceContext(), stmtStats);
			for (final ProjectingStatement ps : dmls.getDerivedInfo().getAllNestedQueries()) {
				final StatementAnalysis<ProjectingStatement> nested = (StatementAnalysis<ProjectingStatement>) build(tee.getPersistenceContext(), null, freq,
						ps);
				visitor.beginStmt(nested);
				visitStatement(tee.getPersistenceContext(), nested);
				visitor.endStmt(nested);
			}
			visitor.endStmt(stmtStats);
		}
	}

	private void visitStatement(SchemaContext db, StatementAnalysis<?> stmtStats) {
		try {
			stmtStats.visit(visitor);
		} catch (final SchemaException se) {
			final String sql = stmtStats.getStatement().getSQL(db);
			throw new SchemaException(Pass.PLANNER, "Unable to obtain stats (freq=" + stmtStats.frequency + ") from " + sql, se);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private StatementAnalysis<?> build(SchemaContext db, String sql, int freq, DMLStatement dmls) {
		StatementAnalysis<?> out = null;
		if (dmls instanceof SelectStatement) {
			out = new SelectStatementAnalysis(db, sql, freq, (SelectStatement) dmls);
		} else if (dmls instanceof InsertStatement) {
			out = new InsertStatementAnalysis(db, sql, freq, (InsertStatement) dmls);
		} else if (dmls instanceof UpdateStatement) {
			out = new UpdateStatementAnalysis(db, sql, freq, (UpdateStatement) dmls);
		} else if (dmls instanceof DeleteStatement) {
			out = new DeleteStatementAnalysis(db, sql, freq, (DeleteStatement) dmls);
		} else {
			out = new StatementAnalysis(db, sql, freq, dmls);
		}
		return out;
	}

	@Override
	public void onException(String sql, SourcePosition sp,
			Throwable t) {
		logger.error("An error occured when analyzing [" + sp.getLineInfo().toString() + "]: " + sql, t);
	}

	@Override
	public void onNotice(String sql, SourcePosition sp, String message) {
		logger.warn(message + " [" + sp.getLineInfo().toString() + "]: " + sql);
	}

}
