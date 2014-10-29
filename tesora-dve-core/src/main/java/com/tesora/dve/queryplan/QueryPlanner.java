package com.tesora.dve.queryplan;

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
import java.util.List;
import java.util.concurrent.Callable;
import com.tesora.dve.clock.NoopTimingService;
import com.tesora.dve.clock.Timer;
import com.tesora.dve.clock.TimingService;
import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.groupmanager.CacheInvalidationMessage;
import com.tesora.dve.groupmanager.GroupTopicPublisher;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.parser.InputState;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.PlanningResult;
import com.tesora.dve.sql.parser.PreparePlanningResult;
import com.tesora.dve.sql.parser.SqlStatistics;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.CachedPreparedStatement;
import com.tesora.dve.sql.schema.cache.PlanCacheUtils;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionPlanOptions;
import com.tesora.dve.sql.util.Pair;

public class QueryPlanner {
	
	private static final boolean noisyErrors = false;

    enum PlannerTime {PLANNER_BUILDPLAN}

	static public Pair<QueryPlan,InputState> computeQueryPlan(final byte[] sqlCommand, final Charset cs, SSConnection connMgr) throws PEException {
		final SchemaContext sc = connMgr.getSchemaContext();
		SchemaContext.setThreadContext(sc);
		sc.refresh();
		try {
			PlanningResult results = buildPlanningResult(new Callable<PlanningResult>() {

				@Override
				public PlanningResult call() throws Exception {
					return InvokeParser.buildPlan(sc, sqlCommand, cs, null);
				}

			}, connMgr);
			return new Pair<QueryPlan,InputState>(buildPlan(results,connMgr,sc),(results == null ? null : results.getInputState()));
		} finally {
			SchemaContext.clearThreadContext();
		}
	}

	static public Pair<QueryPlan,InputState> computeQueryPlan(final InputState sqlCommand, SSConnection connMgr) throws PEException {
		final SchemaContext sc = connMgr.getSchemaContext();
		SchemaContext.setThreadContext(sc);
		sc.refresh();
		try {
			PlanningResult results = buildPlanningResult(new Callable<PlanningResult>() {

				@Override
				public PlanningResult call() throws Exception {
					return InvokeParser.buildPlan(sc,sqlCommand);
				}

			}, connMgr);
			return new Pair<QueryPlan,InputState>(buildPlan(results, connMgr, sc), (results == null ? null : results.getInputState()));
		} finally {
			SchemaContext.clearThreadContext();
		}
	}

	public static PreparedPlan prepareStatement(final byte[] sqlCommand, final Charset cs, SSConnection connMgr, final String stmtID) throws PEException {
		final SchemaContext sc = connMgr.getSchemaContext();
		SchemaContext.setThreadContext(sc);
		sc.refresh();
		try {
			PlanningResult results = buildPlanningResult(new Callable<PlanningResult>() {

				@Override
				public PlanningResult call() throws Exception {
					return InvokeParser.buildPlan(sc, sqlCommand, cs, stmtID);
				}

			}, connMgr);
			PreparePlanningResult ppr = (PreparePlanningResult) results;
			return new PreparedPlan(buildPlan(results, connMgr, sc),
					ppr.getPrepareSQL(),
					ppr.getCachedPlan());
		} finally {
			SchemaContext.clearThreadContext();
		}
	}
	
	public static void registerPreparedStatement(SSConnection connMgr, String stmtID, PreparedPlan prepd) throws PEException {
		// store the original stmt on the connection in case we need to rebuild it
		// and then cache the plan in the pstmt cache
		PlanCacheUtils.registerPreparedStatementPlan(connMgr.getSchemaContext(), (CachedPreparedStatement) prepd.getUnboundPlan(), prepd.getOriginalSQL(), connMgr.getConnectionId(), stmtID, false);
	}
	
	public static void destroyPreparedStatement(SSConnection connMgr, String stmtID) {
		SqlStatistics.incrementCounter(StatementType.CLOSE_PREPARE);
		PlanCacheUtils.destroyPreparedStatement(connMgr.getSchemaContext(),stmtID);
	}
	
	public static QueryPlan buildPreparedPlan(SSConnection connMgr, String stmtID, List<Object> params) throws PEException {
        Timer buildPlanTime = startPlanTimer(PlannerTime.PLANNER_BUILDPLAN);
		SchemaContext cntxt = connMgr.getSchemaContext();
		SchemaContext.setThreadContext(cntxt);
		cntxt.refresh();
		try {
			PlanningResult result = InvokeParser.bindPreparedStatement(cntxt, stmtID, params);
			SqlStatistics.incrementCounter(StatementType.EXEC_PREPARE);
			return buildPlan(result,connMgr,cntxt);
		} finally {
            buildPlanTime.end();
            SchemaContext.clearThreadContext();
		}
	}
	
	static PlanningResult buildPlanningResult(Callable<PlanningResult> toCall, SSConnection connMgr) throws PEException {
        Timer buildPlanTime = startPlanTimer(PlannerTime.PLANNER_BUILDPLAN);

		try {
			return toCall.call();
		} catch (PEException pe) {
			if (noisyErrors) pe.printStackTrace();
			throw pe;
		} catch (Throwable t) {
			if (isFiltered(t,connMgr))
				return null;
			if (noisyErrors) t.printStackTrace();
			if (t instanceof SchemaException) {
				SchemaException se = (SchemaException) t;
				if (se.getErrorInfo() != null)
					throw se;
			}
			throw new PESQLException("Unable to build plan - " + t.getMessage(), t);
		} finally {
            buildPlanTime.end();
        }
	}

    @SuppressWarnings("rawtypes")
	private static Timer startPlanTimer(Enum cat) {
        return Singletons.require(TimingService.class, NoopTimingService.SERVICE).startSubTimer(cat);
    }

    static private boolean isFiltered(Throwable t, SSConnection connMgr) {
		if (!connMgr.getConnectionContext().hasFilter())
			return false;
		if (t instanceof SchemaException) {
			SchemaException se = (SchemaException) t;
			if (se.getErrorInfo().getCode() == AvailableErrors.TABLE_DNE) {
				List<UnqualifiedName> names = new ArrayList<UnqualifiedName>();
				for(Object o : se.getErrorInfo().getParams()) {
					names.add(new UnqualifiedName((String)o));
				}
				return connMgr.getConnectionContext().isFilteredTable(new QualifiedName(names));				
			}
		}
		return false;
	}
	
	public static void invalidateCache(CacheInvalidationRecord cir) {
		if (cir == null) 
			return;
        Singletons.require(GroupTopicPublisher.class).publish(new CacheInvalidationMessage(cir));
	}
	
	private static QueryPlan buildPlan(PlanningResult planningResult,
			SSConnection connMgr, SchemaContext sc) throws PEException {
		if (planningResult == null) {
			return new QueryPlan();
		}
		final List<ExecutionPlan> plans = planningResult.getPlans();
		final QueryPlan plan = new QueryPlan();
		plan.setInputStatement(planningResult.getOriginalSQL());
		final ExecutionPlanOptions opts = new ExecutionPlanOptions();
		final int lastExecutionPlanIndex = plans.size() - 1;
		Long accUpdateCount = null;
		for (int epIdx = 0; epIdx <= lastExecutionPlanIndex; ++epIdx) {
			final ExecutionPlan ep = plans.get(epIdx);
			ep.logPlan(sc, "on conn " + connMgr.getName(), null);			
			final List<QueryStepOperation> steps = ep.schedule(opts, connMgr, sc);
			final int numQuerySteps = steps.size();
			for (int qsIdx = 0; qsIdx < numQuerySteps; ++qsIdx) {
				final QueryStepOperation qs = steps.get(qsIdx);
				plan.addStep(qs);
			}

			Long anyUpdate = ep.getUpdateCount(sc);
			if (anyUpdate != null) 
				accUpdateCount = anyUpdate;
			
			if (epIdx == lastExecutionPlanIndex) {
				if (ep.useRowCount()) {
					plan.setUseRowCount();
				}
			}
		}
		plan.setTrueUpdateCount(accUpdateCount);
		plan.setRuntimeUpdateCountAdjustment(opts);
		// the query plan is created - do any cleanup on the context
		sc.cleanupPostPlanning();
		
		return plan;
	}
			
 }
