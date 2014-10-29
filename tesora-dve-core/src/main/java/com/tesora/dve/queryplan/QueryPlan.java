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


import com.tesora.dve.clock.NoopTimingService;
import com.tesora.dve.clock.Timer;
import com.tesora.dve.clock.TimingService;
import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.common.logutil.LogSubject;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.transform.execution.ExecutionPlanOptions;
import com.tesora.dve.sql.transform.execution.LateBindingUpdateCountFilter.RuntimeUpdateCounter;

/**
 * <b>QueryPlan</b> lays out the steps required to execute a user's SQL query.
 * <p/>
 * A <b>QueryPlan</b> is built up of a series of {@link QueryStep}s, with each
 * step having one or more operations (setup, execute, cleanup) and zero or more 
 * dependencies (steps that must be executed before it).
 * <p/>
 * Each step is executed in sequence by calling {@link QueryPlan#executeStep(SSConnection)}
 * and {@link QueryPlan#getMoreResults()}.  At each step, the <b>QueryPlan</b> makes 
 * available any results of executing the step (see {@link QueryPlan#getResultCollector()}).
 * <p/>
 * See {@link QueryStepBasicTest} for examples of how to build a <b>QueryPlan</b>.
 * 
 */
public class QueryPlan implements LogSubject {
	
	static Logger logger = Logger.getLogger( QueryPlan.class );

    TimingService timingService = Singletons.require(TimingService.class, NoopTimingService.SERVICE);
    enum TimingDesc {QUERYPLAN_STEP_ROUND_TRIP, QUERYPLAN_STEP_BEFORE, QUERYPLAN_STEP_AFTER }


    QueryStepOperation root;
    
	SSConnection ssCon;
	
	// sometimes the update count from the last step is incorrect, as when 
	// a multivalue insert has been planned (the last step may have only one value in it)
	// if the planner knows the true count, use that instead
	Long trueUpdateCount = null;
	RuntimeUpdateCounter lateBindingUpdateCounter = null;
	// sometimes the actual update count is a row count, i.e. insert into select
	boolean useRowCount = false;
	
	// original sql - maybe be only a prefix
	String inputStatement;
	
	/**
	 * Constructs an empty <b>QueryPlan</b>
	 */
	public QueryPlan() {
	}
	
	public QueryPlan(QueryStepOperation op) throws PEException {
		addStep(op);
	}
	
	/**
	 * Adds a step to the <b>QueryPlan</b>
	 * 
	 * @param step step to add
	 * @return itself for inline calling
	 */
	public QueryPlan addStep(QueryStepOperation op) {
		if (root == null)
			root = op;
		else {
			op.addRequirement(root);
			root = op;
		}
		return this;
	}
	
	public void setInputStatement(String sql) {
		if (sql == null) return;
		if (sql.length() > 1024)
			inputStatement = sql.substring(0, 1024);
		else
			inputStatement = sql;
	}
	
	public String getInputStatement() {
		return inputStatement;
	}
	
	public boolean isEmpty() {
		return root == null;
	}
 	
	/**
	 * Executes the current step in the <b>QueryPlan</b>.
	 * 
	 * @param ssCon1 {@link SSConnection} for the user's session
	 * @param resultConsumer 
	 * @return <b>true</b> if the query returned results
	 * @throws Throwable 
	 */
	public void executeStep(SSConnection ssCon1, DBResultConsumer resultConsumer) throws Throwable {
		this.ssCon = ssCon1;

		if (root == null)
			throw new PEException("Attempt to execute empty (or exhausted) QueryPlan)");
		
        //TODO: There is a lot of similarity between the ExecutionLogger and the Timer service, maybe merge them?
        Timer stepTime = timingService.startSubTimer(TimingDesc.QUERYPLAN_STEP_ROUND_TRIP);
        Timer preStep = stepTime.newSubTimer(TimingDesc.QUERYPLAN_STEP_BEFORE);

		ExecutionLogger beforeLogger = ssCon.getExecutionLogger().getNewLogger("BeforePlanExecute");
		if (!ssCon.isAutoCommitMode() && !ssCon.hasActiveTransaction()) {
			ssCon.userBeginTransaction();
		}
		
		boolean txnNeeded = root.requiresTransaction() && root.hasRequirements(); 
		if (txnNeeded) 
			ssCon.autoBeginTransaction(false);
		beforeLogger.end();
        preStep.end();
		try {
			root.execute(new ExecutionState(ssCon1), resultConsumer);
			if (trueUpdateCount != null) 
				resultConsumer.setNumRowsAffected(trueUpdateCount);
			// for certain operations we have to accumulate the update count from dependent steps (i.e. REPLACE INTO)
			if (lateBindingUpdateCounter != null) 
				resultConsumer.setNumRowsAffected(adjustUpdateCount(resultConsumer.getUpdateCount()));
		} catch (PEException e) {
			ExecutionLogger afterLogger = ssCon.getExecutionLogger().getNewLogger("AfterPlanExecuteOnException");
			try {
				try {
                    //TODO: this check is all or none, but apparently 'some do', hence the special casing. -sgossard
					if (ssCon.getDBNative().exceptionAbortsTxn(e)) {
						ssCon.onTransactionAbortedByJDBC();
						txnNeeded = false;
					}
					if (txnNeeded)
						ssCon.autoRollbackTransaction();

                    Exception root = e.rootCause();
                    boolean rootCauseIsSQLErrorMessage = (root instanceof PESQLStateException);

                    if (ssCon.hasActiveXATransaction() && rootCauseIsSQLErrorMessage){
                        PESQLStateException sqlState = (PESQLStateException)root;
                        if (sqlState.isXAFailed()){
                            ssCon.autoRollbackTransaction(true);
                        }
                    }

				} catch (Exception e1) {
					// ignore ?
				}
				throw e;
			} finally {
				afterLogger.end();
			}
		}

        Timer afterTime = stepTime.newSubTimer(TimingDesc.QUERYPLAN_STEP_AFTER);
		ExecutionLogger afterLogger = ssCon.getExecutionLogger().getNewLogger("AfterPlanExecute");
		if (txnNeeded)
			ssCon.autoCommitTransaction();
		afterLogger.end();
        afterTime.end();
        stepTime.end();
	}
	
	// This method is used to add the dependent step update counts to the final tally 
	// used by REPLACE INTO (See commit message on PE-852)
	private long adjustUpdateCount(long in) {
		if (lateBindingUpdateCounter == null) {
			return in;
		}

		if (lateBindingUpdateCounter.getRuntimeUpdateCountAccumulation()) {
			return lateBindingUpdateCounter.incrementBy(in);
		}

		return lateBindingUpdateCounter.getValue();
	}
	
	public void setTrueUpdateCount(Long v) {
		trueUpdateCount = v;
	}

	public void setRuntimeUpdateCountAdjustment(final ExecutionPlanOptions options) {
		lateBindingUpdateCounter = options.getRuntimeUpdateCounter();
	}
	
	public void setUseRowCount() {
		useRowCount = true;
	}
	
	@Override
	public String describeForLog() {
		return getInputStatement();
	}
	
	public QueryStepOperation getRootOperation() {
		return root;
	}
	
}
