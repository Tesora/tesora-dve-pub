package com.tesora.dve.tools.analyzer.sources;

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

import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.tools.analyzer.Analyzer;
import com.tesora.dve.tools.analyzer.AnalyzerSource;
import com.tesora.dve.tools.analyzer.Emulator;
import com.tesora.dve.tools.analyzer.SourcePosition;
import com.tesora.dve.tools.analyzer.jaxb.DbAnalyzerCorpus;
import com.tesora.dve.tools.analyzer.jaxb.InsertTuples;
import com.tesora.dve.tools.analyzer.jaxb.StatementInsertIntoValuesType;
import com.tesora.dve.tools.analyzer.jaxb.StatementNonInsertType;
import com.tesora.dve.tools.analyzer.jaxb.StatementPopulationType;

public class FrequenciesSource extends AnalyzerSource {

	public static final String FREQUENCY_TAG = "freq";
	public static final String DB_TAG = "db";

	private final DbAnalyzerCorpus freqs;
	private final String descr;

	public FrequenciesSource(DbAnalyzerCorpus sft) {
		this.freqs = sft;
		this.descr = "FrequenciesSource using corpus:" + sft.getDescription();
	}

	@Override
	public String getDescription() {
		return descr;
	}

	@Override
	public void analyze(Analyzer a) throws Throwable {
		final boolean emulator = a instanceof Emulator;
		a.setSource(this);
		final ParserInvoker pi = a.getInvoker();
		String cdb = null;
		int counter = 0;
		for (final StatementPopulationType spt : freqs.getPopulation()) {
			final LineInfo li = new LineInfo(++counter, null, 1);

			if ((cdb == null) || !cdb.equals(spt.getDb())) {
				cdb = spt.getDb();
				pi.parseOneLine(li, "use " + cdb);
			}
			if (InsertIntoValuesStatement.class.getSimpleName().equals(spt.getKind())) {
				/*
				 * Corpus files do not store INSERT values.
				 * We can ignore INSERTs in the Emulator (replay) mode.
				 * For other Analyzer types we rebuild the statements
				 * replacing values with parameters.
				 */
				final StatementInsertIntoValuesType siivt = (StatementInsertIntoValuesType) spt;
				if (emulator) {
					a.onNotice(siivt.getInsertPrefix() + " VALUES (...)", new SourcePosition(li), "INSERT satement ignored");
					continue;
				}

				final StringBuilder paramBuf = new StringBuilder();
				paramBuf.append("(");
				for (int c = 0; c < siivt.getColWidth(); c++) {
					if (c > 0) {
						paramBuf.append(",");
					}
					paramBuf.append("?");
				}
				paramBuf.append(")");
				final String params = paramBuf.toString();
				for (final InsertTuples it : siivt.getPopulation()) {
					li.addIntOption(FREQUENCY_TAG, it.getTuplePop());
					li.addStringOption(DB_TAG, cdb);
					final StringBuilder buf = new StringBuilder();
					buf.append(siivt.getInsertPrefix()).append(" VALUES ");
					for (int i = 0; i < it.getTupleCount(); i++) {
						if (i > 0) {
							buf.append(", ");
						}
						buf.append(params);
					}
					pi.parseOneLine(li, buf.toString());
				}

			} else {
				li.addIntOption(FREQUENCY_TAG, spt.getFreq());
				li.addStringOption(DB_TAG, cdb);
				final StatementNonInsertType snit = (StatementNonInsertType) spt;
				pi.parseOneLine(li, snit.getStmt());
			}
		}

		a.onFinished();
		a.setSource(null);
	}

	@Override
	public void closeSource() {
		// noop
	}

	@Override
	public SourcePosition convert(LineInfo li) {
		return new FrequenciesSourcePosition(li);
	}

	private static class FrequenciesSourcePosition extends SourcePosition {

		public FrequenciesSourcePosition(LineInfo li) {
			super(li);
		}

		@Override
		public void describe(PrintStream ps) {
			final Integer freqObj = getLineInfo().getIntOption(FREQUENCY_TAG);
			final int v = (freqObj == null ? 1 : freqObj.intValue());
			final String db = getLineInfo().getStringOption(DB_TAG);
			ps.println();
			ps.println("-- Found " + v + " times in corpus (on db " + db + ")--------------------------");
		}
	}

}
