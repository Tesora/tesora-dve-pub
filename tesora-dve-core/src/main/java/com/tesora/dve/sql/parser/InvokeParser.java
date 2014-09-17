package com.tesora.dve.sql.parser;

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


import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.tesora.dve.common.PECharsetUtils;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.PlannerStatisticType;
import com.tesora.dve.sql.PlannerStatistics;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CandidateCachedPlan;
import com.tesora.dve.sql.schema.cache.PlanCacheUtils;
import com.tesora.dve.sql.schema.cache.PlanCacheUtils.PlanCacheCallback;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.statement.CacheableStatement;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.session.TransactionStatement;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variables.KnownVariables;

public class InvokeParser {

	public static final Logger sqlLogger = Logger.getLogger("sql.logger");
	public static final Logger logger = Logger.getLogger(InvokeParser.class);

	public static final long defaultLargeInsertThreshold = 1024 * 1024;
	
	public static String parseOneLine(InputState line, ParserOptions opts) throws Exception {
		TranslatorUtils utils = new TranslatorUtils(opts, null, line);
		PE parser = buildParser(line, utils);
		Object firstPass = parser.sql_statements().getTree();
		Exception any = utils.buildError();
		if (any != null)
			throw any;
		return utils.displayTree(null, firstPass);
	}

	public static sql2003Lexer buildLexer(InputState inputStatement, Utils utils) {
		ANTLRStringStream input = inputStatement.buildNewStream();
		sql2003Lexer lexer = new sql2003Lexer(input);
		lexer.setUtils(utils);
		return lexer;
	}

	private static PE buildParser(sql2003Lexer lexer, TranslatorUtils utils) {
		// CommonTokenStream tokens = new CommonTokenStream(lexer);
		TokenRewriteStream tokens = new TokenRewriteStream(lexer);
		PE parser = new PE(tokens);
		parser.setUtils(utils);
		return parser;
	}

	private static PE buildParser(InputState line, TranslatorUtils utils) {
		return buildParser(buildLexer(line, utils), utils);
	}

	public static InputState buildInputState(String icmd, SchemaContext pc) {
		long maxLen =  
			KnownVariables.LARGE_INSERT_CUTOFF.getValue(pc == null ? null : pc.getConnection().getVariableSource()).longValue();
		if (icmd.length() > maxLen)
			return new ContinuationInputState(icmd,maxLen);
		return new InitialInputState(icmd);
	}
	
	public static ParseResult parse(InputState icmd, ParserOptions opts, SchemaContext pc) throws ParserException {
		preparse(pc);
		return parse(icmd, opts, pc, Collections.emptyList());
	}

	private static void preparse(SchemaContext pc) throws ParserException {
		if (pc != null) {
			// before we do anything, figure out if the current user is a
			// tenant, and if so, if they have been suspended
			pc.getPolicyContext().checkEnabled();
		}
	}

	private static ParseResult parse(InputState input, ParserOptions opts, SchemaContext pc, List<Object> parameters)
			throws ParserException {
		// debug log is set only for non tests
		if (pc != null) {
			pc.setOptions(opts);
			pc.setParameters(parameters);
			SchemaContext.threadContext.set(pc);
		}
		Pair<TranslatorUtils, List<Statement>> result = null;
		if (pc != null && pc.getIntraStmtState().isUnderLockTable()) {
			result = parseFastInsert(pc, opts, input);
		}
		if (result == null)
			result = parse(pc, opts, input);
		List<Statement> stmts = result.getSecond();
		TranslatorUtils utils = result.getFirst();
		if (stmts.isEmpty())
			stmts.add(new EmptyStatement(""));
		try {
			if (!opts.isTSchema())
				utils.assignPositions();
		} catch (Throwable t) {
			throw new ParserException(Pass.SECOND, "Unable to complete parsing for '" + input.describe() + "'", t);
		}
		for (Statement s : stmts) {
			if (logger.isDebugEnabled() && opts.isDebugLog()) {
				if (pc != null)
					logger.debug("  Statement (" + pc.describeContext() + ") => " + s.getSQL(pc, true, true));
				else
					logger.debug("  Statement => " + s.getSQL(pc, true, true));
			}
		}
		// break a self referential chain
		utils.setContext(null);
		return new ParseResult(stmts,utils.getInputState(input));
	}

	@SuppressWarnings("unchecked")
	private static Pair<TranslatorUtils, List<Statement>> parseFastInsert(SchemaContext pc, ParserOptions opts,
			InputState icmd) {
		// first of all, make sure it's an actual insert statement
		if (!icmd.isInsert())
			return null;
		TranslatorUtils utils = new TranslatorUtils(opts, pc, icmd);
		PE parser = buildParser(icmd, utils);
		if (pc != null)
			pc.setTokenStream(parser.getTokenStream(),icmd.getCommand());
		List<Statement> stmts = null;
		List<List<ExpressionNode>> continuedInsert = null;
		try {
			if (icmd.getCurrentPosition() == 0) {
				stmts = Collections.singletonList(parser.fast_insert_statement().s);
			} else {
				continuedInsert = parser.fast_continuation_insert_value_list().l;
				utils.popScope();
			}
		} catch (EnoughException ee) {
			return new Pair<TranslatorUtils,List<Statement>>(utils, Collections.singletonList(((Statement)ee.getInsertStatement())));
		} catch (Throwable t) {
			// basically, just return null and try again
			return null;
		}
		ParserException any = utils.buildError();
		if (any != null)
			return null;
		if (stmts == null) {
			stmts = new ArrayList<Statement>();
			if (continuedInsert == null || continuedInsert.isEmpty()) {
				stmts.add(new TransactionStatement(TransactionStatement.Kind.COMMIT));
			} else {
				stmts.add(utils.buildInsertStatement(continuedInsert,false, TransactionStatement.Kind.COMMIT, null, false));
			}	
		}
		return new Pair<TranslatorUtils, List<Statement>>(utils, stmts);
	}

	public static Type parseType(final SchemaContext pc, final ParserOptions opts, final String typeDescriptor) {
		final InputState icmd = buildInputState(typeDescriptor, pc);
		final TranslatorUtils utils = new TranslatorUtils(opts, pc, icmd);
		final PE parser = buildParser(icmd, utils);
		try {
			return parser.type_description().type;
		} catch (final RecognitionException e) {
			throw new PECodingException("Could not parse the type descriptor: '" + typeDescriptor + "'", e);
		}
	}

	public static ExpressionNode parseExpression(SchemaContext pc, String input) {
		InputState icmd = buildInputState(input,pc);
		ParserOptions opts = ParserOptions.NONE.setDebugLog(true).setResolve().setFailEarly().setActualLiterals();
		TranslatorUtils utils = new TranslatorUtils(opts,pc,icmd);
		PE parser = buildParser(icmd,utils);
		try {
			return parser.value_expression().expr;
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, "Unable to parser expression '" + input + "'",t);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static Pair<TranslatorUtils, List<Statement>> parse(SchemaContext pc, ParserOptions opts, InputState input) {
		TranslatorUtils utils = new TranslatorUtils(opts, pc, input);
		PE parser = buildParser(input, utils);
		if (pc != null)
			pc.setTokenStream(parser.getTokenStream(), input.getCommand());
		List<Statement> stmts = null;
		List<List<ExpressionNode>> continuedInsert = null;
		try {
			if (input.getCurrentPosition() == 0) {
				stmts = parser.sql_statements().stmts;
			} else {
				continuedInsert = parser.continuation_insert_value_list().l;
				utils.popScope();
			}
		} catch (EnoughException ee) {
			return new Pair<TranslatorUtils,List<Statement>>(utils, Collections.singletonList(((Statement)ee.getInsertStatement())));
		} catch (ParserException pe) {
			throw pe;
		} catch (Throwable t) {
			throw new ParserException(Pass.SECOND, "Unable to parse '" + input.describe() + "'", t);
		}
		ParserException any = utils.buildError();
		if (any != null)
			throw any;
		if (stmts == null) {
			stmts = new ArrayList<Statement>();
			if (continuedInsert == null || continuedInsert.isEmpty()) {
				stmts.add(new TransactionStatement(TransactionStatement.Kind.COMMIT));
			} else {
				stmts.add(utils.buildInsertStatement(continuedInsert,false, TransactionStatement.Kind.COMMIT, null, false));
			}	
		}
		return new Pair<TranslatorUtils, List<Statement>>(utils, stmts);
	}

	public static List<Statement> parse(String line, SchemaContext pc) throws ParserException {
		return parse(line, pc, Collections.emptyList());
	}

	public static List<Statement> parse(String line, SchemaContext pc, List<Object> params, ParserOptions options) throws ParserException {
		return parse(buildInputState(line,pc), options, pc, params).getStatements();		
	}
	
	public static List<Statement> parse(String line, SchemaContext pc, List<Object> params) throws ParserException {
		// the tests were all written to use latin1
		ParserOptions options = ParserOptions.NONE.setDebugLog(true).setResolve().setFailEarly();
		return parse(line,pc,params,options);
	}

	public static ParseResult parse(byte[] line, SchemaContext pc, Charset cs)
			throws ParserException {
		preparse(pc);
		ParserOptions options = ParserOptions.NONE.setDebugLog(true).setResolve().setFailEarly();

		String lineStr = PECharsetUtils.getString(line, cs, true);
		if (lineStr != null) {
			lineStr = StringUtils.strip(lineStr, new String(Character.toString(Character.MIN_VALUE)));
			return parse(buildInputState(lineStr,pc), options, pc);
		}
		return parameterizeAndParse(pc, options, line, cs);
	}

	public static ListOfPairs<Statement,List<Object>> buildParameterizedCommands(String in, Charset cs, SchemaContext pc) {
		// note that we are _not_ setting resolve here
		ParserOptions options = ParserOptions.NONE.setDebugLog(false);
		ParseResult results = parse(buildInputState(in,pc), options, pc);
		if (results.hasMore())
			throw new ParserException(Pass.FIRST, "Unable to parameterize for invalid chars - input too large");
		List<Statement> stmts = parse(buildInputState(in,pc), options, pc).getStatements();
		ListOfPairs<Statement,List<Object>> out = new ListOfPairs<Statement,List<Object>>();
		for (Statement s : stmts)
			try {
				out.add(s,s.extractParameters(pc,PECharsetUtils.latin1, cs));
			} catch (PEException pe) {
				throw new ParserException(Pass.FIRST, "Unable to extract parameters for input stmt '" + in
						+ "' in order to handle character set " + cs.name(), pe);
			}
		return out;
	}

	private static ParseResult parameterizeAndParse(SchemaContext pc, ParserOptions options, byte[] line,
			Charset cs) throws ParserException {
		String singleByteEncoded = PECharsetUtils.getString(line, PECharsetUtils.latin1, false);

		logParse(pc, singleByteEncoded, null);

		ListOfPairs<Statement,List<Object>> parameterized = buildParameterizedCommands(singleByteEncoded, cs, pc);
		ArrayList<Statement> out = new ArrayList<Statement>();
		for (Pair<Statement, List<Object>> p : parameterized) {
			DMLStatement dmls = (DMLStatement) p.getFirst();
			String msql = dmls.getSQL(pc, false, true);
			List<Object> params = p.getSecond();
			byte[] modstmt = msql.getBytes(PECharsetUtils.latin1);
			String orig = PECharsetUtils.getString(modstmt, cs, true);
			if (orig == null)
				throw new ParserException(Pass.FIRST,
						"Unable to parameterize SQL statement to handle characters invalid for character set "
								+ cs.name());
			orig = StringUtils.strip(orig, new String(Character.toString(Character.MIN_VALUE)));
			out.addAll(parse(buildInputState(orig,pc), options, pc, params).getStatements());
		}
		return new ParseResult(out,null);
	}

	public static PlanningResult preparePlan(SchemaContext pc, InputState input, ParserOptions options, String pstmtID) throws PEException {
		ParseResult pr = parse(input, options, pc);
		if (pr.getStatements().size() > 1)
			throw new PEException("Cannot prepare more than one statement at a time");
		Statement toPrepare = pr.getStatements().get(0);
		return Statement.prepare(pc, toPrepare, pc.getBehaviorConfiguration(), pstmtID, input.getCommand());
	}
	
	public static PlanningResult buildPlan(SchemaContext pc, InputState input, ParserOptions options, PlanCacheCallback ipcb) throws PEException {
		PlanCacheCallback pcb = (ipcb == null ? logCacheCallback : ipcb);
		List<ExecutionPlan> plans = null;
		boolean tryCache = pc.getSource().canCachePlans(pc) && !pc.getIntraStmtState().isUnderLockTable();
		CandidateCachedPlan ccp = null;
		if (!pc.getSource().isPlanCacheEmpty() && input.getCommand() != null) {
			ccp = PlanCacheUtils.getCachedPlan(pc, input.getCommand(), pcb);
			if (ccp.getPlan() != null) {
				plans = new ArrayList<ExecutionPlan>();
				plans.add(ccp.getPlan());
			} else if (!ccp.tryCaching()) {
				tryCache = false;
			}
		}
		if (plans == null) {
			ParseResult pr = parse(input, options, pc);
			plans = new ArrayList<ExecutionPlan>();
			Statement first = null;
			boolean explain = false;
			for (Statement s : pr.getStatements()) {
				if (first == null) first = s;
				if (s.isExplain()) {
					explain = true;
					plans.add(buildExplainPlan(pc,s,input.getCommand()));
				} else {
					plans.add(Statement.getExecutionPlan(pc,s,pc.getBehaviorConfiguration(),input.getCommand()));
				}
			}
			if (pcb != null && input.getCommand() != null && !explain)
				pcb.onMiss(input.getCommand());
			if (!explain && input.getCommand() != null && plans.size() == 1 && tryCache && first instanceof CacheableStatement) {
				PlanCacheUtils.maybeCachePlan(pc,pc.getSource(),(CacheableStatement)first, plans.get(0), input.getCommand(),
						ccp == null ? null : ccp.getShrunk());
			}
			if (ccp != null && ccp.getShrunk() == null && pc.getCurrentDatabase(false) != null) {
				if (first instanceof DMLStatement) {
					// we have to count this late because the candidate parser can only handle dml statements.
					PlannerStatistics.increment(PlannerStatisticType.UNCACHEABLE_CANDIDATE_PARSE);					
				}
			}
			return new PlanningResult(plans,pr.getInputState(),input.getCommand());
		}
		// clear the warnings
		pc.getConnection().getMessageManager().clear();
		return new PlanningResult(plans,null,input.getCommand());
	}

	// if we have an explain - we should try to match to the plan cache
	private static ExecutionPlan buildExplainPlan(SchemaContext sc, Statement s, String origSQL) throws PEException {
		// if the explain is for raw statistics, or a regular explain - then we can try the plan cache
		// otherwise not so much
		if (s.getExplain().tryCache()) {
			String sql = origSQL.substring(s.getSourceLocation().getPositionInLine());
			CandidateCachedPlan ccp = null;
			if (!sc.getSource().isPlanCacheEmpty()) {
				ccp = PlanCacheUtils.getCachedPlan(sc, sql, null);
				if (ccp.getPlan() != null) {
					ExecutionPlan actual = ccp.getPlan();
					ExecutionPlan expep = new ExecutionPlan(null,actual.getValueManager(), StatementType.EXPLAIN);
					expep.getSequence().append(actual.generateExplain(sc,s,sql));
					return expep;
				}
			}
		}
		// if we're still here - we have to build the old fashioned way
		return Statement.getExecutionPlan(sc,s,sc.getBehaviorConfiguration(),origSQL);
	}
	
	// continuation version
	public static PlanningResult buildPlan(SchemaContext pc, InputState stmt) throws PEException {
		if (pc !=null )
			SchemaContext.threadContext.set(pc);
		preparse(pc);
		ParserOptions options = ParserOptions.NONE.setDebugLog(true).setResolve();
		PlanningResult result = buildPlan(pc, stmt, options, null);
		for(ExecutionPlan ep : result.getPlans()) 
			SqlStatistics.incrementCounter(ep.getStatementType());
		return result;
	}
	
	public static PlanningResult buildPlan(SchemaContext pc, byte[] line, Charset cs, String pstmtID)
			throws PEException {
		boolean isPrepare = (pstmtID != null);
		if (pc != null)
			SchemaContext.threadContext.set(pc);
		preparse(pc);
		ParserOptions options = ParserOptions.NONE.setDebugLog(true).setResolve().setFailEarly();
		if (isPrepare)
			options = options.setPrepare().setActualLiterals();

		PlanningResult result = null;
		
		String lineStr = PECharsetUtils.getString(line, cs, true);
		if (lineStr == null) {
			// bad characters - not a candidate for caching
			// if we get this for a prepared stmt, just give up for now
			if (isPrepare)
				throw new PEException("Invalid prepare request: bad characters");
			ParseResult pr = parameterizeAndParse(pc, options, line, cs);
			List<Statement> stmts = pr.getStatements();
			List<ExecutionPlan> plans = new ArrayList<ExecutionPlan>();
			for (Statement s : stmts) {
				plans.add(Statement.getExecutionPlan(pc,s,pc.getBehaviorConfiguration(),lineStr));
			}
			result = new PlanningResult(plans, pr.getInputState(),lineStr);
		} else {
			lineStr = StringUtils.strip(lineStr, new String(Character.toString(Character.MIN_VALUE)));

			logParse(pc, lineStr, pstmtID);

			InputState input = buildInputState(lineStr, pc);
			
			if (isPrepare) {
				if (input.getCommand() == null)
					throw new PEException("Prepare stmt is too long (greater than " + input.getThreshold() + " characters)");
				result = preparePlan(pc, input, options, pstmtID);
			} else
				result = buildPlan(pc, input, options, null);
		}
		for(ExecutionPlan ep : result.getPlans()) 
			SqlStatistics.incrementCounter(ep.getStatementType());
		return result;
	}

	public static PlanningResult bindPreparedStatement(SchemaContext sc, String stmtID, List<?> params) throws PEException {
		// make sure we clear the options
		sc.setOptions(ParserOptions.NONE);
		ExecutionPlan bound = PlanCacheUtils.bindPreparedStatement(sc, stmtID, params);
		SqlStatistics.incrementCounter(bound.getStatementType());
		return new PlanningResult(Collections.singletonList(bound),null,null);
	}
	
	public static PreparePlanningResult reprepareStatement(SchemaContext sc, String rawSQL, String stmtID) throws PEException {
		// need to replan
		ParserOptions options = ParserOptions.NONE.setDebugLog(true).setResolve().setPrepare().setActualLiterals();
		InputState input = buildInputState(rawSQL,sc);
		return (PreparePlanningResult) preparePlan(sc, input, options, stmtID);
	}
	
	private static void logParse(SchemaContext pc, String inline, String pstmtID) {
		boolean isPrepare = (pstmtID != null);
		String line = inline;
		if (isPrepare)
			line = "PREPARE " + line;
		if (logger.isDebugEnabled()) {
			if (pc != null)
				logger.debug("Begin " + (isPrepare ? "" : pstmtID) + " parse:(" + pc.describeContext() + ") " + line);
			else
				logger.debug("Begin parse:" + line);
		}
		
		if (sqlLogger.isDebugEnabled()) 
			logSql(pc,line);
	}

	public static void enableSqlLogging(boolean enabled) {
		sqlLogger.setLevel(enabled ? Level.DEBUG : Level.OFF);

		logger.info("Set SQL Logging log level to " + sqlLogger.getLevel().toString());
	}
	
	public static boolean isSqlLoggingEnabled() {
		return sqlLogger.isDebugEnabled();
	}
	
	public static void logSql(SchemaContext pc, String line) {
		if (pc != null)
			sqlLogger.debug("(" + pc.describeContext() + ") " + line);
		else
			sqlLogger.debug(line);		
	}
	
	private static final PlanCacheCallback logCacheCallback = new PlanCacheCallback() {

		private final Logger logger = Logger.getLogger(PlanCacheUtils.class);
		
		@Override
		public void onHit(String stmt) {
			if (logger.isDebugEnabled())
				logger.debug("PlanCache hit: " + stmt);
		}
			
		@Override
		public void onMiss(String stmt) {
			if (logger.isDebugEnabled())
				logger.debug("PlanCache miss: " + stmt);
		}
		
	};
	

}
