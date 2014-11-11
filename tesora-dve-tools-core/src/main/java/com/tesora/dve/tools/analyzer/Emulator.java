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

import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.parser.CandidateParser;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheLimits;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.CachedPreparedStatement;
import com.tesora.dve.sql.schema.cache.PlanCacheKey;
import com.tesora.dve.sql.schema.cache.PlanCacheUtils;
import com.tesora.dve.sql.schema.cache.RegularCachedPlan;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaSourcePlanCache;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;

/**
 * The emulator just replays statements from a source.
 */
public class Emulator extends Analyzer {

	/**
	 * A lightweight plan cache.
	 */
	protected class EmulatorPlanCache implements SchemaSourcePlanCache {

		private final Map<PlanCacheKey, RegularCachedPlan> cache = new HashMap<PlanCacheKey, RegularCachedPlan>();

		@Override
		public boolean isPlanCacheEmpty() {
			return cache.isEmpty();
		}

		@Override
		public boolean canCachePlans(SchemaContext sc) {
			return true;
		}

		@Override
		public void putCachedPlan(RegularCachedPlan cp) {
			cache.put(cp.getKey(), cp);
		}

		@Override
		public void clearCachedPlan(RegularCachedPlan pck) {
			cache.remove(pck);
		}

		@Override
		public RegularCachedPlan getCachedPlan(SchemaContext sc, PlanCacheKey pck) {
			return cache.get(pck);
		}

		@Override
		public CachedPreparedStatement getPreparedStatement(PlanCacheKey pck) {
			return null;
		}

		@Override
		public CachedPreparedStatement getPreparedStatement(SchemaContext sc, int connID, String stmtID) throws PEException {
			return null;
		}

		@Override
		public void putPreparedStatement(CachedPreparedStatement cps, int connID, String stmtID, String rawSQL, boolean reregister) throws PEException {
			// noop
		}

		@Override
		public void clearPreparedStatement(int connID, String stmtID) {
			// noop
		}

		@Override
		public void invalidate(SchemaCacheKey<?> sck) {
			// noop
		}

		@Override
		public void onCacheLimitUpdate(CacheSegment cs, CacheLimits limits) {
			// noop
		}
	}

	protected final boolean suppressDuplicates;
	protected final EmulatorPlanCache planCache;
	protected final AnalyzerCallback cb;

	public Emulator(AnalyzerOptions opts, AnalyzerCallback cb) throws Throwable {
		super(opts);

		if (cb == null) {
			throw new IllegalArgumentException();
		}

		this.cb = cb;
		suppressDuplicates = super.getOptions().isSuppressDuplicatesEnabled();

		planCache = new EmulatorPlanCache();
	}

	@Override
	public void onStatement(String sql, SourcePosition sp, Statement s) throws Throwable {
		if ((s instanceof EmptyStatement) || !s.isDML()) {
			return;
		}

		final DMLStatement dmls = (DMLStatement) s;
		// convert the source position to connection id
		final LineInfo tli = sp.getLineInfo();
		if (tli.getConnectionID() != 0) {
			// force usage of connection id
			tli.setLineNumber(0);
		}

		// we're going to try to plan now
		try {
			CandidateParser cp = null;
			final boolean tryDups = suppressDuplicates && (tee.getPersistenceContext().getCurrentDatabase(false) != null);
			if (tryDups) {
				cp = new CandidateParser(sql);
				if (!cp.shrink()) {
					cp = null;
				} else {
					// see if we get a cache hit
					final PlanCacheKey pck = PlanCacheUtils.buildCacheKey(cp.getShrunk(), tee.getPersistenceContext().getCurrentDatabase(), tee
							.getPersistenceContext().getPolicyContext());
					final RegularCachedPlan ccp = planCache.getCachedPlan(tee.getPersistenceContext(), pck);
					if ((ccp != null) && ccp.isValid(tee.getPersistenceContext(), cp.getLiterals())) {
						return;
					}
				}
			}
			final ExecutionPlan ep = Statement.getExecutionPlan(tee.getPersistenceContext(), dmls);
			if (tryDups) {
				PlanCacheUtils.maybeCachePlan(tee.getPersistenceContext(), planCache, dmls, ep, sql, cp);
			}
			if (!passesRedistCut(ep)) {
				return;
			}
			cb.onResult(new AnalyzerPlanningResult(tee.getPersistenceContext(), sql, sp, dmls, ep));
		} catch (final Throwable t) {
			cb.onResult(new AnalyzerPlanningError(tee.getPersistenceContext(), sql, sp, dmls, t));
		}
	}

	@Override
	public void onException(String sql, SourcePosition sp, Throwable t) {
		cb.onResult(new AnalyzerPlanningError(tee.getPersistenceContext(), sql, sp, null, t));
	}

	@Override
	public void onNotice(String sql, SourcePosition sp, String message) {
		cb.onResult(new AnalyzerPlanningNotice(tee.getPersistenceContext(), sql, sp, null, message));
	}

	private boolean passesRedistCut(ExecutionPlan ep) {
		final int redistStepCutoff = this.getOptions().getRedistributionCutoff();
		if (redistStepCutoff < 0) {
			return true;
		}

		final int redistStepCount = ep.getSequence().getRedistributionStepCount(tee.getPersistenceContext());

		return (redistStepCount > redistStepCutoff);
	}

}
