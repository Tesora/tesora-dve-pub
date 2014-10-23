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


import static com.tesora.dve.tools.analyzer.StatementCounter.DMLCounters.DELETE;
import static com.tesora.dve.tools.analyzer.StatementCounter.DMLCounters.INSERT_INTO_SELECT;
import static com.tesora.dve.tools.analyzer.StatementCounter.DMLCounters.INSERT_INTO_VALUES;
import static com.tesora.dve.tools.analyzer.StatementCounter.DMLCounters.SELECT;
import static com.tesora.dve.tools.analyzer.StatementCounter.DMLCounters.TRUNCATE;
import static com.tesora.dve.tools.analyzer.StatementCounter.DMLCounters.UNION;
import static com.tesora.dve.tools.analyzer.StatementCounter.DMLCounters.UPDATE;
import static com.tesora.dve.tools.analyzer.StatementCounter.NonDMLCounters.ALTER;
import static com.tesora.dve.tools.analyzer.StatementCounter.NonDMLCounters.CREATE;
import static com.tesora.dve.tools.analyzer.StatementCounter.NonDMLCounters.DROP;
import static com.tesora.dve.tools.analyzer.StatementCounter.NonDMLCounters.GRANT;
import static com.tesora.dve.tools.analyzer.StatementCounter.NonDMLCounters.SESSION;
import static com.tesora.dve.tools.analyzer.StatementCounter.NonDMLCounters.SHOW;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.errmap.ErrorMapper;
import com.tesora.dve.errmap.FormattedErrorInfo;
import com.tesora.dve.exceptions.HasErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.parser.CandidateParser;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.AlterStatement;
import com.tesora.dve.sql.statement.ddl.DropStatement;
import com.tesora.dve.sql.statement.ddl.GrantStatement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.statement.session.SessionStatement;
import com.tesora.dve.tools.analyzer.jaxb.DbAnalyzerCorpus;
import com.tesora.dve.tools.analyzer.jaxb.InsertTuples;
import com.tesora.dve.tools.analyzer.jaxb.StatementInsertIntoValuesType;
import com.tesora.dve.tools.analyzer.jaxb.StatementNonDMLType;
import com.tesora.dve.tools.analyzer.jaxb.StatementNonInsertType;
import com.tesora.dve.tools.analyzer.jaxb.StatementPopulationType;
import com.tesora.dve.tools.analyzer.stats.EnumStatsCollector;
import com.tesora.dve.tools.analyzer.stats.IntegerHistogram;

public class StatementCounter extends Analyzer {

	private static final Logger logger = Logger.getLogger(StatementCounter.class);

	private final HashMap<String, NonInsertEntry> byParam = new HashMap<String, NonInsertEntry>();
	private final HashMap<String, NonInsertEntry> byShrunk = new HashMap<String, NonInsertEntry>();

	private final HashMap<String, InsertEntry> inserts = new HashMap<String, InsertEntry>();

	private final HashMap<String, StatementNonDMLType> otherByInput = new HashMap<String, StatementNonDMLType>();
	private final HashMap<String, StatementNonDMLType> otherByShrunk = new HashMap<String, StatementNonDMLType>();

	private final EmitOptions emitOptions;

	private final CachingInvoker myInvoker;

	private final DbAnalyzerCorpus corpus;

	private final File corpusFile;
	private final File checkpointFile;
	private final int checkpointInterval;

	// errors are parse errors
	// warnings are other interesting things -
	// something we don't fully support across the entire planning chain
	static enum GlobalCounters {
		LINES, PROCESSED_STATEMENTS, EMPTY_STATEMENTS, SHRINK_CACHE_ADDS, SHRINK_CACHE_HITS, SHRINK_CACHE_MISSES, ERRORS, WARNINGS
	}

	private static final EnumStatsCollector<GlobalCounters> globalCounters = new EnumStatsCollector<>(GlobalCounters.class);

	static enum NonDMLCounters {
		SESSION, ALTER, SHOW, CREATE, GRANT, DROP, UNKNOWN
	}

	private static final EnumStatsCollector<NonDMLCounters> nonDMLBreakout = new EnumStatsCollector<>(NonDMLCounters.class);

	static enum DMLCounters {
		DELETE, UPDATE, SELECT, UNION, TRUNCATE, INSERT_INTO_SELECT, INSERT_INTO_VALUES, UNKNOWN
	}

	private static final EnumStatsCollector<DMLCounters> dmlBreakout = new EnumStatsCollector<>(DMLCounters.class);

	private static final IntegerHistogram literalCounts = new IntegerHistogram();

	private final BufferedWriter errorLog;
	private final PrintStream outputStream;
	
	private final boolean stackTraces;

	public StatementCounter(AnalyzerOptions opts, File corpusFile, File checkpointFile, int checkpointInterval,
			File errorFileName, PrintStream outputStream) throws Throwable {
		super(opts);

		emitOptions = EmitOptions.NONE.analyzerLiteralsAsParameters();

		myInvoker = new CachingInvoker(this);

		corpus = new DbAnalyzerCorpus();

		this.corpusFile = corpusFile;
		this.checkpointFile = checkpointFile;
		this.checkpointInterval = checkpointInterval;
		this.errorLog = new BufferedWriter(new FileWriter(errorFileName));
		this.outputStream = outputStream;
		this.stackTraces = getOptions().isVerboseErrors();
	}

	@Override
	public void setSource(AnalyzerSource as) {
		if (as != null) {
			corpus.setDescription(as.getDescription());
		}
	}

	@Override
	public ParserInvoker getInvoker() {
		return myInvoker;
	}

	@Override
	public void onFinished() throws PEException {
		checkpoint(true);
	}

	public void close() {
		try {
			errorLog.close();
		} catch (final IOException e) {
			logger.warn("Failed to close the error log file");
		}
	}

	private void checkpoint(boolean last) throws PEException {
		final long total = globalCounters.getOccurances(GlobalCounters.LINES);

		if (last || ((total % checkpointInterval) == 0)) {
			outputStream.println("Writing checkpoint file, total stmts: "
					+ total);

			final String corpusString = PEXmlUtils.marshalJAXB(corpus);

			final File prevFile = new File(checkpointFile.getAbsolutePath() + ".prev");
			if (prevFile.exists()) {
				prevFile.delete();
			}

			if (checkpointFile.exists()) {
				checkpointFile.renameTo(prevFile);
			}

			PEFileUtils.writeToFile(checkpointFile, corpusString, true);
		}

		if (last) {
			final String corpusString = PEXmlUtils.marshalJAXB(sortByFrequency());
			PEFileUtils.writeToFile(corpusFile, corpusString, true);
			printSummary(outputStream);
		}

		flushErrorLog();
	}

	private void flushErrorLog() throws PEException {
		try {
			errorLog.flush();
		} catch (final IOException ioe) {
			throw new PEException("Unable to flush error log", ioe);
		}
	}

	private static void printSummary(final PrintStream outputStream) {
		nonDMLBreakout.printTo(
				"\nNon-DML Breakout         : %s\n",
				"    %-20s : %s\n",
				outputStream
				);

		dmlBreakout.printTo(
				"\nDML Breakout             : %s\n",
				"    %-20s : %s\n",
				outputStream
				);

		outputStream.println();
		outputStream.println("Literal counts / shrunk DML statements");
		outputStream.printf("    %-20s : %s\n", "count", literalCounts.getTotalOccurances());
		outputStream.printf("    %-20s : %s\n", "min", literalCounts.getMinimum());
		outputStream.printf("    %-20s : %4.4f\n", "avg", literalCounts.getAverage());
		outputStream.printf("    %-20s : %4.4f\n", "std dev(approx)", literalCounts.getStandardDeviation());
		outputStream.printf("    %-20s : %s\n", "90.0%", literalCounts.getPercentile(0.900d));
		outputStream.printf("    %-20s : %s\n", "99.0%", literalCounts.getPercentile(0.990d));
		outputStream.printf("    %-20s : %s\n", "99.9%", literalCounts.getPercentile(0.999d));
		outputStream.printf("    %-20s : %8.8f%%\n", "percentile of 100", literalCounts.findPercentile(100) * 100.0d);
		outputStream.printf("    %-20s : %s\n", "max", literalCounts.getMaximum());

		globalCounters.printTo(
				"\nSummary                  : \n",
				"    %-20s : %s\n",
				outputStream
				);
	}

	@Override
	public void onStatement(String sql, SourcePosition sp, Statement s) throws Throwable {
		saveIntermediateCheckpoint();
		globalCounters.increment(GlobalCounters.PROCESSED_STATEMENTS);
		if (s instanceof EmptyStatement) {
			globalCounters.increment(GlobalCounters.EMPTY_STATEMENTS);
			return;
		}
		if (!s.isDML()) {
			trackNonDML(s, sql);
			return;
		}
		final Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
		emitter.setOptions(emitOptions);
		final StringBuilder buf = new StringBuilder();
		final DMLStatement dmls = (DMLStatement) s;

		final DMLCounters incCounter = lookupDMLCounter(dmls);

		if (dmls instanceof InsertIntoValuesStatement) {
			final InsertIntoValuesStatement iivs = (InsertIntoValuesStatement) dmls;
			emitter.emitInsertPrefix(tee.getPersistenceContext(), iivs, buf);
			final String prefix = buf.toString();
			InsertEntry ie = inserts.get(prefix);
			if (ie == null) {
				final Database<?> db = dmls.getDatabase(tee.getPersistenceContext());
				ie = new InsertEntry(corpus, prefix, iivs.getColumnSpecification().size(), iivs.getClass()
						.getSimpleName(), (db == null ? null : db.getName().get()));
				inserts.put(prefix, ie);
			}
			ie.bump(iivs.getValues().size());
		} else {
			emitter.emitStatement(tee.getPersistenceContext(), dmls, buf);
			final String p = buf.toString();
			NonInsertEntry se = byParam.get(p);
			if (se == null) {
				String shrunk = null;
				int litCount = -1;
				if (!(dmls instanceof InsertIntoValuesStatement)) {
					final CandidateParser cp = new CandidateParser(sql);
					if (cp.shrink()) {
						shrunk = cp.getShrunk();
						// also verify we get the same number of literals
						final ValueManager valueManager = tee.getPersistenceContext().getValueManager();
						litCount = cp.getLiterals().size();
						if (litCount != valueManager.getNumberOfLiterals()) {
							final ValueManager.CacheStatus cacheStatus = valueManager.getCacheStatus();
							String reason;
							switch (cacheStatus) {
							case NOCACHE_DYNAMIC_FUNCTION:
								reason = "contains a non-cacheable dynamic function";
								break;
							case NOCACHE_TOO_MANY_LITERALS:
								reason = "literal count exceeded configured max_cached_plan_literals";
								break;
							case CACHEABLE:
							default:
								reason = "unknown";
							}
							logError(sql, sp, "Mismatched literal size; parse="
									+ valueManager.getNumberOfLiterals() + "/shrink="
									+ litCount + " , reason=" + reason
									,
									null, false);
						}
					} else {
						logError(sql, sp, "Unable to shrink", null, false);
					}
				}
				final Database<?> db = dmls.getDatabase(tee.getPersistenceContext());
				se = new NonInsertEntry(corpus, sql, dmls.getClass().getSimpleName(), (db == null ? null : db.getName()
						.get()), litCount, incCounter);
				byParam.put(p, se);
				if (shrunk != null) {
					globalCounters.increment(GlobalCounters.SHRINK_CACHE_ADDS);
					byShrunk.put(shrunk, se);
				}
			}
			if (se.populationObject.getLiteralCount() >= 0) {
				literalCounts.sample(se.populationObject.getLiteralCount());
			}
			se.bump(dmlBreakout, literalCounts);
		}
	}

	private DMLCounters lookupDMLCounter(DMLStatement dmls) {
		if (dmls instanceof DeleteStatement) {
			return DELETE;
		} else if (dmls instanceof UpdateStatement) {
			return UPDATE;
		} else if (dmls instanceof SelectStatement) {
			return SELECT;
		} else if (dmls instanceof UnionStatement) {
			return UNION;
		} else if (dmls instanceof TruncateStatement) {
			return TRUNCATE;
		} else if (dmls instanceof InsertIntoValuesStatement) {
			return INSERT_INTO_VALUES;
		} else if (dmls instanceof InsertIntoSelectStatement) {
			return INSERT_INTO_SELECT;
		} else {
			return DMLCounters.UNKNOWN;
		}
	}

	private NonDMLCounters lookupNonDMLCounter(Statement s) {
		NonDMLCounters incCounter;
		if (s instanceof SessionStatement) {
			incCounter = SESSION;
		} else if (s instanceof AlterStatement) {
			incCounter = ALTER;
		} else if (s instanceof SchemaQueryStatement) {
			incCounter = SHOW;
		} else if (s instanceof PECreateStatement) {
			incCounter = CREATE;
		} else if (s instanceof GrantStatement) {
			incCounter = GRANT;
		} else if (s instanceof DropStatement) {
			incCounter = DROP;
		} else {
			incCounter = NonDMLCounters.UNKNOWN;
		}
		return incCounter;
	}

	@Override
	public void onException(String sql, SourcePosition sp, Throwable t) {
		saveIntermediateCheckpoint();
		logError(sql, sp, t.getMessage(), t, true);
	}

	@Override
	public void onNotice(String sql, SourcePosition sp, String message) {
		logger.warn(message + " [" + sp.getLineInfo().toString() + "]: " + sql);
	}

	protected void saveIntermediateCheckpoint() {
		try {
			checkpoint(false);
		} catch (final Throwable ct) {
			logger.error("Unable to save a checkpoint", ct);
		}
	}

	static final String invokerClass = InvokeParser.class.getName();
	static final String invokerMethod = "parse";
	
	/**
	 * This is a simple format:
	 * sql: <the sql>
	 * error | warn: <the message>
	 */
	private void logError(String sql, SourcePosition sp, String message, Throwable cause, boolean error) {
		if (error) {
			globalCounters.increment(GlobalCounters.ERRORS);
		} else {
			globalCounters.increment(GlobalCounters.WARNINGS);
		}

		try {
			final String type = error ? "error" : "warn";
			final String line = "(line=" + sp.getPosition() + ")";
			
			if (message == null && cause instanceof HasErrorInfo) {
				FormattedErrorInfo formatted = ErrorMapper.makeResponse((HasErrorInfo)cause);
				if (formatted != null)
					message = formatted.getErrorMessage();
			}
			
			errorLog.write("---- " + type + " " + line + " ---------------");
			errorLog.newLine();
			errorLog.write("msg: " + message);
			errorLog.newLine();
			errorLog.write("sql: " + sql);
			errorLog.newLine();
			if (error && stackTraces && cause != null) {
				for(StackTraceElement ste : cause.getStackTrace()) {
					errorLog.write("trace: " + ste);
					errorLog.newLine();
					if (invokerClass.equals(ste.getClassName()))
						break;
				}
			}
		} catch (final Throwable ct) {
			logger.error("Unable to write error log record", ct);
		}
	}

	private void trackNonDML(Statement s, String sql) {
		String rep = null;
		if (s instanceof SchemaQueryStatement) {
			final SchemaQueryStatement sqs = (SchemaQueryStatement) s;
			rep = "show " + sqs.getTag();
		} else {
			rep = sql;
		}

		nonDMLBreakout.increment(lookupNonDMLCounter(s));
		trackOther(rep);
	}

	private void trackOther(String sql) {
		StatementNonDMLType nd = null;
		final String shrunk = CandidateParser.shrinkAnything(sql);
		if (shrunk != null) {
			nd = otherByShrunk.get(shrunk);
		}
		if (nd == null) {
			nd = otherByInput.get(sql);
		}
		if (nd == null) {
			nd = new StatementNonDMLType();
			nd.setStmt(sql);
			otherByInput.put(sql, nd);
			if (shrunk != null) {
				otherByShrunk.put(shrunk, nd);
			}
			corpus.getNonDml().add(nd);
		}
		nd.setFreq(nd.getFreq() + 1);
	}

	private DbAnalyzerCorpus sortByFrequency() {
		final MultiMap<Integer, StatementPopulationType> byFrequency = new MultiMap<Integer, StatementPopulationType>();
		for (final StatementPopulationType spt : corpus.getPopulation()) {
			byFrequency.put(spt.getFreq(), spt);
		}
		final List<Integer> freqs = new ArrayList<Integer>(byFrequency.keySet());
		Collections.sort(freqs, Collections.reverseOrder());
		corpus.getPopulation().clear();
		for (final Integer i : freqs) {
			final Collection<StatementPopulationType> sub = byFrequency.get(i);
			if ((sub == null) || sub.isEmpty()) {
				continue;
			}
			corpus.getPopulation().addAll(sub);
		}
		return corpus;
	}

	private static class NonInsertEntry {

		private final StatementNonInsertType populationObject;
		private final DMLCounters dmlCategory;

		public NonInsertEntry(DbAnalyzerCorpus reportObject, String rep, String kind, String db, int literals, DMLCounters incCounter) {
			this.populationObject = new StatementNonInsertType();
			this.populationObject.setStmt(rep);
			this.populationObject.setKind(kind);
			this.populationObject.setDb(db);
			this.populationObject.setLiteralCount(literals);
			reportObject.getPopulation().add(populationObject);
			dmlCategory = incCounter;
		}

		public void bump(EnumStatsCollector<DMLCounters> dmlBreakout, IntegerHistogram literalCounts) {
			populationObject.setFreq(populationObject.getFreq() + 1);
			dmlBreakout.increment(dmlCategory);
			literalCounts.sample(populationObject.getLiteralCount(), 1L);
		}
	}

	private static class InsertEntry {

		private final StatementInsertIntoValuesType populationObject;
		private final HashMap<Integer, InsertTuples> byWidth;

		public InsertEntry(DbAnalyzerCorpus reportObject, String prefix, int colwidth, String kind, String db) {
			this.populationObject = new StatementInsertIntoValuesType();
			this.populationObject.setInsertPrefix(prefix);
			this.populationObject.setKind(kind);
			this.populationObject.setDb(db);
			this.populationObject.setColWidth(colwidth);
			reportObject.getPopulation().add(populationObject);
			byWidth = new HashMap<Integer, InsertTuples>();
		}

		public void bump(int width) {
			InsertTuples tuple = byWidth.get(width);
			if (tuple == null) {
				tuple = new InsertTuples();
				tuple.setTupleCount(width);
				tuple.setTuplePop(0);
				byWidth.put(width, tuple);
				populationObject.getPopulation().add(tuple);
			}
			tuple.setTuplePop(tuple.getTuplePop() + 1);
			populationObject.setFreq(populationObject.getFreq() + 1);
		}

	}

	private static class CachingInvoker extends AnalyzerInvoker {

		public CachingInvoker(Analyzer a) {
			super(a);
		}

		@Override
		public boolean omit(LineInfo info, String sql) {
			globalCounters.increment(GlobalCounters.LINES);
			final StatementCounter sc = (StatementCounter) sink;
			if (CandidateParser.isInsert(sql)) {
				return false;
			}
			final CandidateParser cp = new CandidateParser(sql);
			if (cp.shrink()) {
				final String shrunk = cp.getShrunk();
				final NonInsertEntry se = sc.byShrunk.get(shrunk);
				if (se != null) {
					globalCounters.increment(GlobalCounters.SHRINK_CACHE_HITS);
					sc.saveIntermediateCheckpoint();
					se.bump(dmlBreakout, literalCounts);
					return true;
				}

				globalCounters.increment(GlobalCounters.SHRINK_CACHE_MISSES);
			}
			return false;
		}
	}
}
